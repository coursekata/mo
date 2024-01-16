"""Base class for organizing data."""


import os
from abc import ABC
from pathlib import Path
from typing import Iterable

import polars as pl

from mo.core.interfaces import IOrganizer, IReader, IWriter
from mo.core.typing import PathStr
from mo.core.writers import ParquetWriter


class BaseOrganizer(IOrganizer, ABC):
    """Base class for organizing data."""

    name: str
    """The name of the data type."""

    reader: IReader
    """The reader for the data type."""

    writer: IWriter = ParquetWriter()
    """The writer for the data type."""

    def find_paths(self, input: PathStr | Iterable[PathStr]) -> Iterable[Path]:
        """Find all paths to source files."""
        for path in [input] if isinstance(input, (str, Path)) else input:
            path = Path(path)
            if path.is_dir():
                yield from path.rglob(f"{self.name}.csv")
                yield from path.rglob(f"{self.name}.parquet")
            elif path.exists() and path.name == f"{self.name}.csv":
                yield path

    def find_dirs(self, input: PathStr | Iterable[PathStr]) -> Iterable[Path]:
        """Find all paths to source directories."""
        for path in [input] if isinstance(input, (str, Path)) else input:
            path = Path(path)
            if path.is_dir():
                yield path

    def remove_empty_dirs(self, path: PathStr) -> None:
        """Remove empty directories."""
        for root, dirs, _ in os.walk(path, topdown=False):
            for dir in dirs:
                dir_path = os.path.join(root, dir)
                if not os.listdir(dir_path):
                    os.rmdir(dir_path)
        if not os.listdir(path):
            os.rmdir(path)

    def __call__(
        self, input: PathStr | Iterable[PathStr], output_dir: PathStr, keep_source: bool = False
    ) -> pl.LazyFrame:
        """Organize item response information."""
        paths = self.find_paths(input)
        output_file = Path(output_dir) / f"{self.name}.parquet"
        df = self.merge(paths)
        df = self.transform(df)
        self.writer(df, output_file)

        if not keep_source:
            self.clean_up(input, paths, output_file)

        return df

    def merge(self, paths: Iterable[Path]) -> pl.LazyFrame:
        df = pl.concat([self.reader(path) for path in paths], how="diagonal").unique()
        return df

    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        return df

    def clean_up(
        self, input: PathStr | Iterable[PathStr], paths: Iterable[Path], output_file: Path
    ):
        for path in paths:
            if path.is_file() and not path.samefile(output_file):
                path.unlink()
        for dir in self.find_dirs(input):
            self.remove_empty_dirs(dir)
