from collections.abc import Collection
from pathlib import Path

import polars as pl

from mo.metrics.domain.institution_type import InstitutionType

StrPath = str | Path


class LoadManifestUseCase:
    input_path: Path
    institution_types: Collection[InstitutionType] = [i for i in InstitutionType]

    def __init__(
        self,
        input_path: StrPath,
        institution_types: Collection[InstitutionType] | None = None,
    ):
        self.input_path = Path(input_path)
        self.institution_types = institution_types or self.institution_types

    def execute(self) -> pl.DataFrame:
        return (
            pl.read_csv(self.input_path)
            .pipe(self.clean_manifest, institution_types=self.institution_types)
            .pipe(self.add_academic_year, column="first_response")
            .pipe(self.add_years_taught)
        )

    @staticmethod
    def clean_manifest(
        df: pl.DataFrame, institution_types: Collection[InstitutionType]
    ) -> pl.DataFrame:
        def convert_date(column: str) -> pl.Expr:
            return (
                pl.col(column)
                .str.strptime(pl.Date, "%Y-%m-%d %H:%M:%S%.f", strict=False)
                .alias(column)
            )

        return df.filter(
            # exclude test classes and study groups
            pl.col("class_type") == "real",
            # only keep requested insitution types
            pl.col("type").is_in(institution_types),
            # exclude classes with no students
            pl.col("students") > 1,
        ).with_columns(
            convert_date("first_response"),
            convert_date("last_response"),
            convert_date("date_created"),
            convert_date("completed_at"),
        )

    @staticmethod
    def add_academic_year(
        df: pl.DataFrame, column: str, output_column: str = "academic_year"
    ) -> pl.DataFrame:
        if output_column in df.columns:
            raise ValueError(f"Column '{output_column}' already exists in dataframe")

        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in dataframe")

        df = df.with_columns(
            pl.when(pl.col(column).dt.month() >= 7)
            .then(pl.col(column).dt.year().cast(pl.Int32))
            .otherwise(pl.col(column).dt.year() - 1)
            .alias(output_column)
        )

        return df.with_columns(
            pl.format(
                "{}-{}",
                pl.col(output_column),
                pl.col(output_column) + 1,
            ).alias(output_column)
        )

    @staticmethod
    def add_years_taught(
        df: pl.DataFrame,
        by: str | list[str] = "instructor_id",
        year_col: str = "academic_year",
        output_column: str = "years_taught",
    ) -> pl.DataFrame:
        return df.with_columns(pl.col(year_col).rank(method="dense").over(by).alias(output_column))
