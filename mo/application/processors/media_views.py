from copy import copy
from typing import final

import polars as pl

from mo.application.processors import dtypes
from mo.application.processors.base import BaseProcessor


@final
class MediaViewsProcessor(BaseProcessor):
    """Process media view data from a CSV."""

    input_schema = copy(dtypes.media_views)
    output_schema = copy(dtypes.media_views)

    def __init__(self):
        super().__init__()
        self.output_schema["log"] = self.output_schema["log_json"]
        del self.output_schema["log_json"]

    def clean(self, df: pl.LazyFrame | pl.DataFrame) -> pl.LazyFrame:
        return (
            df.lazy()
            .filter(pl.col("class_id").is_not_null())
            .filter(pl.col("student_id").is_not_null())
            .filter(pl.col("dt_started").is_not_null())
            .rename({"log_json": "log"})
        )
