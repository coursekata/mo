"""Organize class metadata."""

from pathlib import Path
from typing import Iterable

import polars as pl
from polars import exceptions

from mo.core.interfaces import IReader, IWriter
from mo.core.organize.base import BaseOrganizer
from mo.core.read.classes import ClassesReader, ManifestReader
from mo.core.writers import ParquetWriter

parquet_writer = ParquetWriter()
classes_reader = ClassesReader()
manifest_reader = ManifestReader()


class ClassesOrganizer(BaseOrganizer):
    """Organize class metadata."""

    name = "classes"

    def __init__(
        self,
        writer: IWriter = parquet_writer,
        reader: IReader = classes_reader,
        manifest_reader: IReader = manifest_reader,
    ) -> None:
        """Initialize a ClassesOrganizer."""
        self.reader = reader
        self.manifest_reader = manifest_reader
        self.writer = writer

    def merge(self, paths: Iterable[Path]) -> pl.LazyFrame:
        dfs: list[pl.LazyFrame] = []
        for path in paths:
            try:
                # have to collect to force trying to read as manifest.
                # TODO: a faster way to check if manifest would be to read first line
                dfs.append(self.manifest_reader(path).collect().lazy())
            except exceptions.ColumnNotFoundError:
                dfs.append(self.reader(path))

        return pl.concat(dfs, how="diagonal").unique()
