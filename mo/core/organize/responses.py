"""Organize item response information."""

from mo.core.interfaces import IReader, IWriter
from mo.core.organize.base import BaseOrganizer
from mo.core.read.responses import ResponsesReader
from mo.core.writers import ParquetWriter


class ResponsesOrganizer(BaseOrganizer):
    """Organize item response information."""

    name = "responses"

    def __init__(
        self, writer: IWriter = ParquetWriter(), reader: IReader = ResponsesReader()
    ) -> None:
        """Initialize a ResponsesOrganizer."""
        self.reader = reader
        self.writer = writer
