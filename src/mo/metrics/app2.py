import logging
from collections.abc import Callable, Collection
from pathlib import Path
from typing import cast

import polars as pl

from mo.domain.data_format import DataFormat
from mo.domain.data_types import DataType
from mo.logging import setup_logging
from mo.metrics.domain.institution_type import InstitutionType
from mo.metrics.usecases.tag_academic_year_usecase import TagAcademicYearUseCase


def load_data(
    path: Path, data_type: DataType, data_format: DataFormat | None = None
) -> pl.LazyFrame:
    from mo.domain.data_types import SCHEMAS

    data_format = data_format or DataFormat(path.suffix.lstrip("."))
    schema = SCHEMAS[data_type]
    if data_format == DataFormat.CSV:
        df = pl.scan_csv(path, schema_overrides=schema)
    elif data_format == DataFormat.PARQUET:
        df = pl.scan_parquet(path)
    else:
        raise ValueError(f"Unknown data type: {data_type.value}")
    return df.select([k for k in schema])


def load_manifest(
    dir_data: Path,
    dir_data_raw: Path,
    institution_types: Collection[InstitutionType],
    force: bool = False,
    log_level: int | str = logging.DEBUG,
) -> pl.LazyFrame:
    setup_logging(log_level)
    log = logging.getLogger(__name__)

    def _load_manifest(path: Path) -> pl.LazyFrame:
        return (
            load_data(path, DataType.MANIFEST)
            .with_columns(d_first_response=pl.col("first_response").str.split("T").list.get(0))
            .with_columns(d_first_response=pl.col("d_first_response").str.split(" ").list.get(0))
            .cast(pl.Date, strict=False)
        )

    def _merge_nces(dir_data: Path, dir_data_raw: Path, manifest: pl.LazyFrame) -> pl.LazyFrame:
        import pyreadstat

        def _load_data(
            pattern: str, manifest: pl.LazyFrame, process: Callable[[pl.LazyFrame], pl.LazyFrame]
        ) -> pl.LazyFrame:
            dfs: list[pl.LazyFrame] = []
            for path in dir_data_raw.rglob(pattern):
                log.debug(f"Loading and processing {path}")
                df = cast(pl.DataFrame, pl.from_pandas(pyreadstat.read_sas7bdat(path)[0]))  # type: ignore
                dfs.append(
                    manifest.join(
                        process(df.lazy()),
                        how="left",
                        on=["institution_id", "academic_year"],
                    )
                )
            return pl.concat(dfs, how="diagonal_relaxed")

        def _process_membership(df: pl.LazyFrame) -> pl.LazyFrame:
            log.debug("Processing NCES membership data")
            return df.filter(pl.col("TOTAL_INDICATOR") == "Education Unit Total").select(
                nces_id=pl.col("NCESSCH").cast(pl.Int64),
                academic_year=pl.col("SCHOOL_YEAR"),
                students=pl.col("STUDENT_COUNT").cast(pl.UInt32),
            )

        def _process_lunch(df: pl.LazyFrame) -> pl.LazyFrame:
            log.debug("Processing NCES lunch data")
            return df.filter(
                pl.col("DATA_GROUP") == "Free and Reduced-price Lunch Table",
                pl.col("TOTAL_INDICATOR") == "Education Unit Total",
            ).select(
                nces_id=pl.col("NCESSCH").cast(pl.Int64),
                academic_year=pl.col("SCHOOL_YEAR"),
                frl_students=pl.col("STUDENT_COUNT").cast(pl.UInt32),
            )

        def _process_characteristics(df: pl.LazyFrame) -> pl.LazyFrame:
            log.debug("Processing NCES characteristics data")
            title_1_codes = [
                "SWELIGNOPROG",  # eligible, not participating
                "SWELIGSWPROG",  # eligible, participating
                "SWELIGTGPROG",  # eligible, participating in targeted assistance
                "TGELGBNOPROG",  # eligible for targeted assistance, not participating
                "TGELGBTGPROG",  # eligible for targeted assistance, participating
            ]
            nslp_codes = [
                "NSLPCEO",  # participating under Community Eligibility Option
                "NSLPPRO1",  # participating under Provision 1
                "NSLPPRO2",  # participating under Provision 2
                "NSLPPRO3",  # participating under Provision 3
                "NSLPWOPRO",  # participating without provision
            ]
            return df.select(
                nces_id=pl.col("NCESSCH").cast(pl.Int64),
                academic_year=pl.col("SCHOOL_YEAR"),
                title_1=pl.when(pl.col("TITLEI_STATUS").is_in(title_1_codes))
                .then(True)
                .when(pl.col("TITLEI_STATUS") == "NOTTITLE1ELIG")
                .then(False)
                .otherwise(None),
                nslp=pl.when(pl.col("NSLP_STATUS").is_in(nslp_codes))
                .then(True)
                .when(pl.col("NSLP_STATUS") == "NSLPNO")
                .then(False)
                .otherwise(None),
            )

        log.debug("Loading NCES IDs")
        manifest = manifest.join(
            (
                pl.scan_csv(dir_data / "nces-map.csv")
                .select("nces_id", institution_id=pl.col("coursekata_id"))
                .drop_nulls()
            ),
            how="left",
            on="institution_id",
        )

        log.info("Joining NCES data to manifest")
        return (
            manifest.join(
                _load_data("ccd_sch_052_*.sas7bdat", manifest, _process_membership),
                how="left",
                on=["nces_id", "academic_year"],
            )
            .join(
                _load_data("ccd_sch_033_*.sas7bdat", manifest, _process_lunch),
                how="left",
                on=["nces_id", "academic_year"],
            )
            .join(
                _load_data("ccd_sch_129_*.sas7bdat", manifest, _process_characteristics),
                how="left",
                on=["nces_id", "academic_year"],
            )
            .with_columns(pct_frl_students=pl.col("frl_students") / pl.col("students"))
            .with_columns(title_1_or_40_frl=pl.col("title_1") | (pl.col("pct_frl_students") >= 0.4))
            # .unique("class_id")
        )

    def _filter(df: pl.LazyFrame, institution_types: Collection[InstitutionType]) -> pl.LazyFrame:
        return df.filter(pl.col("type").is_in(institution_types))

    output = dir_data / "manifest.parquet"
    if not output.exists() or force:
        log.debug("Loading classes and manifest CSVs")
        df = pl.concat(_load_manifest(p) for p in dir_data_raw.rglob("manifest.csv"))
        tag_input = TagAcademicYearUseCase.Input(data=df, dt_col_name="d_first_response")
        df = TagAcademicYearUseCase().execute(tag_input).data

        # if InstitutionType.K12 in institution_types:
        #     df = _merge_nces(dir_data, dir_data_raw, df)

        log.debug(f"Writing {str(output)}")
        df.collect().write_parquet(output)

    log.debug(f"Loading prepared data, filtered to types: {[i.value for i in institution_types]}")
    return _filter(pl.scan_parquet(output), institution_types)
