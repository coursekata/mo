"""Read page viewing information."""

import polars as pl
from polars.type_aliases import FrameType as FrameType

from mo.core import dtypes
from mo.core.interfaces import IReader
from mo.core.read.null_values import NULL_VALUES
from mo.core.typing import PathStr


class PageViewsReader(IReader):
    """Read page view data from a CSV."""

    def __call__(self, input: PathStr) -> pl.LazyFrame:
        return (
            pl.scan_csv(input, dtypes=dtypes.page_views, null_values=NULL_VALUES)
            .filter(pl.col("class_id").is_not_null())
            .filter(pl.col("student_id").is_not_null())
            .filter(pl.col("dt_accessed").is_not_null())
        )
