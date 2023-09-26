"""Organize page viewing information."""

import polars as pl

from mo.core.interfaces import IReader, IWriter
from mo.core.organize.base import BaseOrganizer
from mo.core.read.page_views import PageViewsReader
from mo.core.writers import ParquetWriter


class PageViewsOrganizer(BaseOrganizer):
    """Organize item response information."""

    name = "page_views"

    def __init__(
        self,
        writer: IWriter = ParquetWriter(),
        reader: IReader = PageViewsReader(),
        long_format: bool = False,
    ) -> None:
        """Initialize a PageViewsOrganizer."""
        self.reader = reader
        self.writer = writer
        self.long_format = long_format

    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        if not self.long_format:
            return df

        drop_from_long = [
            "engaged",
            "idle_brief",
            "idle_long",
            "off_page_brief",
            "off_page_long",
        ]

        # extract tried_again_dt to rows that can be appended
        tried_again_rows = (
            df.drop(drop_from_long, "trace")
            .filter(pl.col("tried_again_dt").is_not_null())
            .rename({"tried_again_dt": "timestamp"})
            .with_columns(
                switched_to=pl.lit("tried_again"),
                dt_switched_to=pl.col("timestamp").cast(pl.Datetime),
            )
            .drop("timestamp")
        )

        # explode the trace without the tried_again_dt's
        exploded_trace = (
            df.drop(drop_from_long, "tried_again_dt")
            .with_columns(
                trace=pl.when(pl.col("trace").str.strip() == "")
                .then("[]")
                .otherwise(pl.col("trace"))
            )
            .with_columns(
                trace=pl.col("trace").str.json_extract(dtype=pl.List(_trace_item))
            )
            .explode("trace")
            .unnest("trace")
            # re-name/order the columns so that we can vstack
            .rename({"timestamp": "dt_switched_to"})
            .select(tried_again_rows.columns)
        )

        return pl.concat([exploded_trace, tried_again_rows])


_trace_item = pl.Struct(
    [
        pl.Field("timestamp", pl.Datetime),
        pl.Field("switched_to", pl.Utf8),
    ]
)
