"""Organize media viewing information."""

from mo.core.interfaces import IReader, IWriter
from mo.core.organize.base import BaseOrganizer
from mo.core.read.media_views import MediaViewsReader
from mo.core.writers import ParquetWriter


class MediaViewsOrganizer(BaseOrganizer):
    """Organize item response information."""

    name = "media_views"

    def __init__(
        self, writer: IWriter = ParquetWriter(), reader: IReader = MediaViewsReader()
    ) -> None:
        """Initialize a MediaViewsOrganizer."""
        self.reader = reader
        self.writer = writer
