"""Interfaces consumed by the core application."""

from abc import abstractmethod
from typing import Iterable, Protocol

import polars as pl

from mo.core.typing import PathStr


class IReader(Protocol):
    """Read data from CSV."""

    @abstractmethod
    def __call__(self, input: PathStr) -> pl.LazyFrame:
        """Read data from a CSV."""


class IWriter(Protocol):
    """Write data to file."""

    @abstractmethod
    def __call__(self, df: pl.LazyFrame, output: PathStr) -> None:
        """Write data to file."""
        pass


class IOrganizer(Protocol):
    """Interface for consolidating and merging data download directories."""

    name: str
    """The name of the data type."""

    reader: IReader
    """The reader for the data type."""

    writer: IWriter
    """The writer for the data type."""

    @abstractmethod
    def __call__(
        self, input: PathStr | Iterable[PathStr], output_dir: PathStr, keep_source: bool = False
    ) -> pl.LazyFrame:
        """Organize a data type from many CSVs to a single file."""
        pass
