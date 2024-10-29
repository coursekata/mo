from typing import final

import polars as pl

from mo.metrics.domain.config import Config
from mo.metrics.usecases.usecase import UseCase


class Input(Config):
    map: pl.LazyFrame
    lunch: pl.LazyFrame
    characteristics: pl.LazyFrame
    membership: pl.LazyFrame


class Output(Config):
    output: pl.LazyFrame


@final
class ProcessNCESUseCase(UseCase):
    Input = Input

    def execute(self, input: Input) -> Output:
        self.log.info("Processing NCES data")
        map = input.map.select("nces_id", institution_id=pl.col("coursekata_id"))

        def filter_map(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.join(map, on="nces_id", how="inner").drop("institution_id")

        membership = filter_map(self._process_membership(input.membership))
        lunch = filter_map(self._process_lunch(input.lunch))
        characteristics = filter_map(self._process_characteristics(input.characteristics))
        merged = (
            membership.join(lunch, on=["nces_id", "academic_year"], how="full", coalesce=True)
            .join(characteristics, on=["nces_id", "academic_year"], how="full", coalesce=True)
            .join(map, on="nces_id", how="inner")
            .with_columns(pct_frl_students=pl.col("frl_students") / pl.col("students"))
            .with_columns(title_1_or_40_frl=pl.col("title_1") | (pl.col("pct_frl_students") >= 0.4))
            .unique(["nces_id", "academic_year"])
        )
        return Output(output=merged)

    def _process_membership(self, df: pl.LazyFrame) -> pl.LazyFrame:
        return df.filter(pl.col("TOTAL_INDICATOR") == "Education Unit Total").select(
            nces_id=pl.col("NCESSCH").cast(pl.Utf8),
            academic_year=pl.col("SCHOOL_YEAR"),
            students=pl.col("STUDENT_COUNT").cast(pl.UInt32),
        )

    def _process_lunch(self, df: pl.LazyFrame) -> pl.LazyFrame:
        return df.filter(
            pl.col("DATA_GROUP") == "Free and Reduced-price Lunch Table",
            pl.col("TOTAL_INDICATOR") == "Education Unit Total",
        ).select(
            nces_id=pl.col("NCESSCH").cast(pl.Utf8),
            academic_year=pl.col("SCHOOL_YEAR"),
            frl_students=pl.col("STUDENT_COUNT").cast(pl.UInt32),
        )

    def _process_characteristics(self, df: pl.LazyFrame) -> pl.LazyFrame:
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
            nces_id=pl.col("NCESSCH").cast(pl.Utf8),
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
