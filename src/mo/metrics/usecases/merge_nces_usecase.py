from collections.abc import Iterable
from functools import reduce
from pathlib import Path
from typing import final

import polars as pl
import pyreadstat

from mo.domain.data_format import DataFormat
from mo.metrics.domain.config import Config
from mo.metrics.usecases.usecase import UseCase


class Input(Config):
    inputs: Iterable[Path]
    output: Path
    output_format: DataFormat = DataFormat.PARQUET
    input_formats: list[DataFormat] = [data_format for data_format in DataFormat]
    allow_unpack: bool = False
    remove_input: bool = False


@final
class MergeNCESUseCase(UseCase):
    Input = Input

    def execute(self, input: Input) -> None:
        self.log.info("Merging NCES data")

        # include the output so that we merge with any previously consolidated data
        input_dirs = [*input.inputs, input.output]

        self.log.debug("Loading lunch data")
        lunch_data = self._load_lunch_data(input_dirs)
        self._write_data(lunch_data, input.output / "nces_lunch.parquet", DataFormat.PARQUET)

        self.log.debug("Loading characteristics data")
        characteristics_data = self._load_characteristics_data(input_dirs)
        self._write_data(
            characteristics_data, input.output / "nces_characteristics.parquet", DataFormat.PARQUET
        )

    def _load_lunch_data(self, input_dirs: list[Path]) -> pl.LazyFrame:
        dfs: list[pl.DataFrame] = [
            pl.from_pandas(pyreadstat.read_sas7bdat(target)[0])  # type: ignore
            for input_dir in input_dirs
            for target in input_dir.rglob("ccd_sch_033_*.sas7bdat")
        ]
        return reduce(lambda x, y: pl.concat([x, y], how="diagonal_relaxed"), dfs).lazy()

    def _load_characteristics_data(self, input_dirs: list[Path]) -> pl.LazyFrame:
        dfs: list[pl.DataFrame] = [
            pl.from_pandas(pyreadstat.read_sas7bdat(target)[0])  # type: ignore
            for input_dir in input_dirs
            for target in input_dir.rglob("ccd_sch_129_*.sas7bdat")
        ]
        return reduce(lambda x, y: pl.concat([x, y], how="diagonal_relaxed"), dfs).lazy()

    def _write_data(self, data: pl.LazyFrame, output_path: Path, output_format: DataFormat) -> None:
        self.log.debug(f"Writing data to {str(output_path)}")
        if output_format is DataFormat.PARQUET:
            data.collect(streaming=True).write_parquet(output_path)
        elif output_format is DataFormat.CSV:
            data.collect(streaming=True).write_csv(output_path)
