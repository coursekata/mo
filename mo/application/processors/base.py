import copy
import logging
from pathlib import Path
from typing import Iterable, Literal

import polars as pl
from polars import exceptions

from mo.application.constants import NULL_VALUES
from mo.application.interfaces.processor import IProcessor


class BaseProcessor(IProcessor):
    def __init__(self):
        self.log = logging.getLogger(self.__class__.__name__)
        if not self.input_schema:
            raise ValueError("`input_schema` must be set")
        if not self.output_schema:
            self.output_schema = self.input_schema
        # make sure we don't modify the original schema
        self.input_schema = copy.copy(self.input_schema)
        self.output_schema = copy.copy(self.output_schema)

    def read(self, input: Path) -> pl.LazyFrame:
        return self.clean(self.raw_read(input))

    def raw_read(self, input: Path) -> pl.LazyFrame:
        try:
            self.log.debug(f"Reading {input}")
            return pl.read_csv(
                input, dtypes=self.input_schema, null_values=NULL_VALUES, try_parse_dates=True
            ).lazy()
        except exceptions.NoDataError:
            return self.template_df()

    def read_output(self, input: Path, format: Literal["csv", "parquet"]) -> pl.LazyFrame:
        df = (
            pl.scan_csv(input, dtypes=self.output_schema, null_values=NULL_VALUES)
            if format == "csv"
            else pl.scan_parquet(input)
        )

        for column, dtype in self.output_schema.items():
            if dtype in df.schema and dtype != df.schema[column]:
                raise ValueError(
                    f"Schema mismatch for `{column}` in {str(input)}:\n"
                    f"  Expected: {dtype}\n"
                    f"  Actual: {df.schema[column]}"
                )

        return df

    def template_df(self) -> pl.LazyFrame:
        """Create a template DataFrame."""
        return pl.DataFrame(schema=self.input_schema).lazy()

    def write(
        self, data: pl.LazyFrame | pl.DataFrame, output: Path, format: Literal["csv", "parquet"]
    ) -> None:
        df = data.lazy().collect()
        df.write_csv(output) if format == "csv" else df.write_parquet(output)

    def merge(
        self, source: Iterable[Path], destination: Path, format: Literal["csv", "parquet"]
    ) -> None:
        df = self.concat(self.read(path) for path in source)
        self.write(df, destination, format)

    def raw_merge(
        self, source: Iterable[Path], destination: Path, format: Literal["csv", "parquet"]
    ) -> None:
        df = self.concat(self.raw_read(path) for path in source)
        self.write(df, destination, format)

    def concat(self, dfs: Iterable[pl.LazyFrame | pl.DataFrame]) -> pl.LazyFrame:
        return pl.concat((df.lazy() for df in dfs), how="diagonal").unique()
