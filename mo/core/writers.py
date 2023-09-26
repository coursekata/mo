"""Writers for MO."""

from pathlib import Path

import polars as pl

from mo.core.interfaces import IWriter
from mo.core.typing import PathStr


class NoopWriter(IWriter):
    """Dummy writer that does nothing."""

    def __call__(self, df: pl.LazyFrame, output: PathStr = ".") -> None:
        """Do nothing."""
        pass


class ParquetWriter(IWriter):
    """Write data to Parquet file."""

    def __call__(self, df: pl.LazyFrame, output: PathStr) -> None:
        """Write data to Parquet file."""
        Path(output).parent.mkdir(parents=True, exist_ok=True)
        df.collect().write_parquet(output)
