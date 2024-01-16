"""Organize all data."""

from typing import Iterable

import polars as pl
from pydantic import BaseModel

from mo.core.organize.classes import ClassesOrganizer
from mo.core.organize.media_views import MediaViewsOrganizer
from mo.core.organize.page_views import PageViewsOrganizer
from mo.core.organize.responses import ResponsesOrganizer
from mo.core.typing import PathStr


class MOData(BaseModel, arbitrary_types_allowed=True):
    classes: pl.LazyFrame
    media_views: pl.LazyFrame
    page_views: pl.LazyFrame
    responses: pl.LazyFrame


class DataOrganizer:
    classes_organizer = ClassesOrganizer()
    media_views_organizer = MediaViewsOrganizer()
    page_views_organizer = PageViewsOrganizer()
    responses_organizer = ResponsesOrganizer()

    def __call__(
        self, input: PathStr | Iterable[PathStr], output_dir: PathStr, keep_source: bool = False
    ) -> MOData:
        """Organize all data."""
        return MOData(
            classes=self.classes_organizer(input, output_dir, keep_source),
            media_views=self.media_views_organizer(input, output_dir, keep_source),
            page_views=self.page_views_organizer(input, output_dir, keep_source),
            responses=self.responses_organizer(input, output_dir, keep_source),
        )
