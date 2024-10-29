from collections.abc import Iterable
from pathlib import Path
from typing import final

import polars as pl

from mo.domain.data_format import DataFormat
from mo.metrics.domain.config import Config
from mo.metrics.usecases.usecase import UseCase


class Input(Config):
    inputs: Iterable[Path]
    output: Path
    output_format: DataFormat = DataFormat.PARQUET
    filter_ids: list[str] = []


@final
class MergeNCESUseCase(UseCase):
    Input = Input

    def execute(self, input: Input) -> None:
        self.log.info("Merging NCES data")
        input_dirs = list(input.inputs)

        self.log.debug("Merging membership data")
        self._merge_data(
            input_dirs,
            "ccd_sch_052_*.csv",
            input.output,
            "nces-membership",
            input.output_format,
            input.filter_ids,
        )

        self.log.debug("Merging lunch data")
        self._merge_data(
            input_dirs,
            "ccd_sch_033_*.csv",
            input.output,
            "nces-lunch",
            input.output_format,
            input.filter_ids,
        )

        self.log.debug("Merging characteristics data")
        self._merge_data(
            input_dirs,
            "ccd_sch_129_*.csv",
            input.output,
            "nces-characteristics",
            input.output_format,
            input.filter_ids,
        )

    def _merge_data(
        self,
        input_dirs: list[Path],
        pattern: str,
        output_dir: Path,
        output: str,
        output_format: DataFormat,
        filter_ids: list[str],
    ) -> None:
        dfs: list[pl.LazyFrame] = [
            self._read_and_filter_data(target, filter_ids)
            for input_dir in input_dirs
            for target in input_dir.rglob(pattern, case_sensitive=False)
        ]

        output_path = output_dir / f"{output}.{output_format.value}"
        if output_path.exists():
            dfs.append(pl.scan_parquet(output_path))

        self._write_data(
            pl.concat(dfs, how="diagonal_relaxed"),
            output_path,
            output_format,
        )

    def _read_and_filter_data(self, input: Path, filter_ids: list[str]) -> pl.LazyFrame:
        df = pl.scan_csv(input)
        if filter_ids:
            return df.filter(pl.col("NCESSCH").is_in(filter_ids))
        return df

    def _write_data(self, data: pl.LazyFrame, output_path: Path, output_format: DataFormat) -> None:
        self.log.debug(f"Writing data to {str(output_path)}")
        if output_format is DataFormat.PARQUET:
            data.collect(streaming=True).write_parquet(output_path)
        elif output_format is DataFormat.CSV:
            data.collect(streaming=True).write_csv(output_path)
