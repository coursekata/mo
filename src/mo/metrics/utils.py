import polars as pl


def convert_mangled_dt_to_date(
    df: pl.LazyFrame, col_name: str, output_col_name: str | None = None
) -> pl.LazyFrame:
    output_col_name = output_col_name or col_name
    return (
        df.with_columns(pl.col(col_name).str.split("T").list.get(0).alias(output_col_name))
        .with_columns(pl.col(output_col_name).str.split("HH").list.get(0).alias(output_col_name))
        .with_columns(pl.col(output_col_name).str.split(" ").list.get(0).alias(output_col_name))
        .with_columns(pl.col(output_col_name).cast(pl.Date, strict=False).alias(output_col_name))
    )
