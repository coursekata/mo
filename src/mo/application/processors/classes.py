from typing import final

import polars as pl

from mo.application.processors import dtypes
from mo.application.processors.base import BaseProcessor


@final
class ClassesProcessor(BaseProcessor):
    """Process class metadata from a CSV."""

    input_schema = dtypes.metadata
    output_schema = dtypes.manifest

    def clean(self, df: pl.LazyFrame | pl.DataFrame) -> pl.LazyFrame:
        rename_map = {
            "teacher_id": "instructor_id",
            "release": "version",
            "course_name": "course",
            "setup_yaml": "book_config",
        }

        return (
            df.lazy()
            .filter(pl.col("class_id").is_not_null())  # type: ignore
            .rename(rename_map)
            .with_columns(
                pl.when(pl.col("course") == "UCLATALL/czi-stats-course")
                .then(pl.lit("Statistics and Data Science:  A Modeling Approach"))
                .otherwise(pl.col("course"))
                .alias("course")
            )
        )
