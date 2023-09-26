"""Read class metadata."""

import json
from typing import Iterable

import polars as pl
from polars.type_aliases import FrameType as FrameType

from mo.core import dtypes
from mo.core.interfaces import IReader
from mo.core.typing import PathStr


class ManifestReader(IReader):
    """Read page view data from a CSV."""

    def __init__(self, status: str = "complete", class_type: str = "real") -> None:
        super().__init__()
        self.status = status
        self.class_type = class_type

    def __call__(self, input: PathStr) -> pl.LazyFrame:
        """Read a "complete list" classes manifest CSV file."""
        return (
            pl.scan_csv(input, dtypes=dtypes.manifest)
            .filter(pl.col("class_id").is_not_null())
            .filter(pl.col("status") == self.status)
            .filter(pl.col("class_type") == self.class_type)
        )


class ClassesReader(IReader):
    """Read page view data from a CSV."""

    def __init__(self, status: str = "complete", class_type: str = "real") -> None:
        super().__init__()
        self.status = status
        self.class_type = class_type

    def __call__(self, input: PathStr) -> pl.LazyFrame:
        """Read a "classes.csv" CSV file from a data download."""
        return (
            pl.scan_csv(input, dtypes=dtypes.metadata)
            .filter(pl.col("class_id").is_not_null())
            .rename(
                {
                    "teacher_id": "instructor_id",
                    "release": "version",
                    "course_name": "course",
                    "setup_yaml": "book_config",
                }
            )
            .with_columns(
                course=pl.when(pl.col("course") == "UCLATALL/czi-stats-course")
                .then("Statistics and Data Science:  A Modeling Approach")
                .otherwise(pl.col("course"))
            )
        )

    @staticmethod
    def pages_from_setup(setup_yaml: str) -> Iterable[str]:
        """Parse a setup_yaml JSON string into a list of pages."""
        try:
            chapters = json.loads(setup_yaml)["chapters"]
            assert isinstance(chapters, Iterable)
        except (json.JSONDecodeError, KeyError, AssertionError):
            chapters = []

        pages: list[str] = []
        for chapter in chapters:  # type: ignore
            if "pages" in chapter and isinstance(chapter["pages"], Iterable):
                for page in chapter["pages"]:  # type: ignore
                    if "name" in page:
                        assert isinstance(page["name"], str)
                        pages.append(page["name"])  # type: ignore

        return pages  # type: ignore
