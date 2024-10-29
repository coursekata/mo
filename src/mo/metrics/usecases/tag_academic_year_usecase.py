from datetime import date
from typing import TypeVar

import polars as pl
from pydantic import BaseModel, ConfigDict

from mo.metrics.domain.config import Config
from mo.metrics.usecases.usecase import UseCase

FrameType = TypeVar("FrameType", pl.DataFrame, pl.LazyFrame)


class Input(Config):
    data: pl.DataFrame | pl.LazyFrame
    dt_col_name: str = "d_activity"


class Output(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    data: pl.LazyFrame


class TagAcademicYearUseCase(UseCase):
    Input = Input

    def __init__(
        self,
        date_col_name: str = "d_activity",
        term_col_name: str = "term",
        year_col_name: str = "academic_year",
    ):
        self.date_col_name = date_col_name
        self.term_col_name = term_col_name
        self.year_col_name = year_col_name

    def execute(self, input: Input) -> Output:
        return Output(
            data=self._tag_term_and_year(
                input.data.lazy(),
                input.dt_col_name,
                self.date_col_name,
                self.term_col_name,
                self.year_col_name,
            )
        )

    def _tag_term_and_year(
        self,
        df: FrameType,
        dt_col_name: str,
        date_col_name: str = "d_activity",
        term_col_name: str = "term",
        year_col_name: str = "academic_year",
    ) -> FrameType:
        date_col = pl.col(date_col_name)
        range(2020, date.today().year + 1)
        return (
            df.with_columns(pl.col(dt_col_name).cast(pl.Date, strict=False).alias(date_col_name))
            # tag by term
            .with_columns(date_col.dt.month().alias(term_col_name))
            .with_columns(pl.col(term_col_name).cut(breaks=[7], labels=["Spring", "Fall"]))
            # tag by year
            .with_columns(
                pl.format(
                    "{}-{}",
                    pl.when(pl.col(term_col_name) == "Fall")
                    .then(date_col.dt.year())
                    .otherwise(date_col.dt.year() - 1),
                    pl.when(pl.col(term_col_name) == "Fall")
                    .then(date_col.dt.year() + 1)
                    .otherwise(date_col.dt.year()),
                ).alias(year_col_name)
            )
            .filter(pl.col(term_col_name).is_not_null() & pl.col(year_col_name).is_not_null())
        )
