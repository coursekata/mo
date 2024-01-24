from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterable, Literal

import polars as pl
from polars.type_aliases import SchemaDict


class IProcessor(ABC):
    """Interface for reading data files."""

    input_schema: SchemaDict
    """The Polars schema for the data file."""

    output_schema: SchemaDict
    """The Polars schema for the cleaned data."""

    @abstractmethod
    def read(self, input: Path) -> pl.LazyFrame:
        """Read and clean the data file."""

    @abstractmethod
    def raw_read(self, input: Path) -> pl.LazyFrame:
        """Read the data file without cleaning."""

    @abstractmethod
    def read_output(self, input: Path, format: Literal["csv", "parquet"]) -> pl.LazyFrame:
        """Read a file that was output by this processor."""

    @abstractmethod
    def clean(self, df: pl.LazyFrame | pl.DataFrame) -> pl.LazyFrame:
        """Clean the data."""

    @abstractmethod
    def write(
        self, data: pl.LazyFrame | pl.DataFrame, output: Path, format: Literal["csv", "parquet"]
    ) -> None:
        """Write the data file."""

    @abstractmethod
    def merge(
        self, source: Iterable[Path], destination: Path, format: Literal["csv", "parquet"]
    ) -> None:
        """Read, clean, and merge the data files."""

    @abstractmethod
    def raw_merge(
        self, source: Iterable[Path], destination: Path, format: Literal["csv", "parquet"]
    ) -> None:
        """Merge the data files without cleaning."""

    @abstractmethod
    def concat(self, dfs: Iterable[pl.LazyFrame | pl.DataFrame]) -> pl.LazyFrame:
        """Concatenate the data frames."""
