"""Organize item response information."""

from mo.core.interfaces import IReader, IWriter
from mo.core.organize.base import BaseOrganizer
from mo.core.read.responses import ResponsesReader
from mo.core.writers import ParquetWriter

parquet_writer = ParquetWriter()
responses_reader = ResponsesReader()


class ResponsesOrganizer(BaseOrganizer):
    """Organize item response information."""

    name = "responses"

    def __init__(
        self, writer: IWriter = parquet_writer, reader: IReader = responses_reader
    ) -> None:
        """Initialize a ResponsesOrganizer."""
        self.reader = reader
        self.writer = writer
