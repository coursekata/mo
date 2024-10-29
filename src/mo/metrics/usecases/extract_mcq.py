from datetime import datetime
from itertools import chain
from pathlib import Path
from typing import final

import polars as pl

from mo.domain.data_types import SCHEMAS, DataType
from mo.metrics.domain.config import Config
from mo.metrics.usecases.usecase import UseCase
from mo.metrics.utils import convert_mangled_dt_to_date


class Input(Config):
    inputs: list[Path]
    output: Path
    end_date: datetime


@final
class ExtractMCQUseCase(UseCase):
    Input = Input

    def execute(self, input: Input) -> None:
        self.log.info("Extracting multiple-choice responses")
        dfs: list[pl.LazyFrame] = []
        for path in chain.from_iterable(
            input_dir.rglob(f"{DataType.RESPONSES}.csv") for input_dir in input.inputs
        ):
            dfs.append(
                pl.scan_csv(path, schema_overrides=SCHEMAS[DataType.RESPONSES])
                .pipe(convert_mangled_dt_to_date, "dt_submitted", "d_submitted")
                .filter(pl.col("lrn_type") == "mcq", pl.col("d_submitted") < input.end_date)
            )
        return pl.concat(dfs, how="diagonal_relaxed").collect().write_parquet(input.output)
