"""Organize media viewing information."""

from mo.core.interfaces import IReader, IWriter
from mo.core.organize.base import BaseOrganizer
from mo.core.read.media_views import MediaViewsReader
from mo.core.writers import ParquetWriter

parquet_writer = ParquetWriter()
media_views_reader = MediaViewsReader()


class MediaViewsOrganizer(BaseOrganizer):
    """Organize media viewing information."""

    name = "media_views"

    def __init__(
        self, writer: IWriter = parquet_writer, reader: IReader = media_views_reader
    ) -> None:
        """Initialize a MediaViewsOrganizer."""
        self.reader = reader
        self.writer = writer
