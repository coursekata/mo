from pathlib import Path
from typing import final

import polars as pl
from pydantic import BaseModel, ConfigDict

from mo.domain.data_types import DataType
from mo.metrics.domain.config import Config
from mo.metrics.usecases.usecase import UseCase


class Input(Config):
    input: Path
    data_types: list[DataType] = [DataType.RESPONSES, DataType.PAGE_VIEWS, DataType.MEDIA_VIEWS]


class Output(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    data: pl.LazyFrame


@final
class GatherActivityUseCase(UseCase):
    Input = Input

    def execute(self, input: Input) -> Output:
        self.log.info("Gathering activity data")
        return Output(data=self._load_data(input.input, input.data_types))

    def _load_data(self, input_path: Path, data_types: list[DataType]) -> pl.LazyFrame:
        dfs: list[pl.LazyFrame] = []
        for data_type in data_types:
            date_time_columns = self._columns_for_type(data_type)
            columns = ["class_id", "student_id"] + date_time_columns
            dfs.append(
                pl.scan_parquet(input_path / f"{data_type.value}.parquet")
                .select(columns)
                # melt the date-time columns into a single 'dt_activity' column
                .unpivot(
                    date_time_columns,
                    index=["class_id", "student_id"],
                    value_name="dt_activity",
                )
                .drop("variable")
                # convert the mangled date-time strings into dates
                .with_columns(d_activity=pl.col("dt_activity").str.split("HH").list.get(0))
                .with_columns(d_activity=pl.col("d_activity").str.split("T").list.get(0))
                .with_columns(d_activity=pl.col("d_activity").str.split(" ").list.get(0))
                .with_columns(d_activity=pl.col("d_activity").cast(pl.Date, strict=False))
                # drop the detritus
                .drop("dt_activity")
                .drop_nulls()
            )

        return pl.concat(dfs, how="vertical")

    def _columns_for_type(self, data_type: DataType) -> list[str]:
        if data_type == DataType.RESPONSES:
            return ["dt_submitted"]
        if data_type == DataType.PAGE_VIEWS:
            return ["dt_accessed", "tried_again_dt"]
        if data_type == DataType.MEDIA_VIEWS:
            return ["dt_started", "dt_last_event"]
        raise ValueError(f"Unsupported data type: {data_type}")
