from copy import copy
from typing import final

import polars as pl

from mo.application.processors import dtypes
from mo.application.processors.base import BaseProcessor


@final
class PageViewsProcessor(BaseProcessor):
    """Process page view data from a CSV."""

    input_schema = copy(dtypes.page_views)
    output_schema = copy(dtypes.page_views)

    def clean(self, df: pl.LazyFrame | pl.DataFrame) -> pl.LazyFrame:
        return (
            df.lazy()
            .filter(pl.col("class_id").is_not_null())
            .filter(pl.col("student_id").is_not_null())
            .filter(pl.col("dt_accessed").is_not_null())
        )
