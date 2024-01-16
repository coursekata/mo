"""Read item response information."""

import polars as pl
from polars import exceptions
from polars.type_aliases import FrameType as FrameType

from mo.core import dtypes
from mo.core.interfaces import IReader
from mo.core.typing import PathStr


class ResponsesReader(IReader):
    """Read response data from a CSV."""

    def __call__(self, input: PathStr) -> pl.LazyFrame:
        exclude = [
            "89296286-f21e-4b08-a9db-2232179b27c1",
            "9ae1e478-fc9d-491e-8a61-530158b0406c",
            "a4c99872-8575-43fa-ad48-22ade9eda0ad",
            "d25a112f-403e-407d-8e9c-7f9a39ad9f58",
            "76b12a49-0209-44e8-bcca-bbbadacc0571",
            "2a49e592-54be-415b-baa8-c136d6888cc3",
            "5ee43530-8f24-4db5-9a95-eeca64b11a90",
            "3622dba6-1ae9-4f05-8632-1181cddd23c6",
            "9591262d-176c-4567-a2c3-c254c51ed677",
            "e7e9185a-d414-445e-a71a-ced42e596523",
            "b2518c54-e2d3-47ff-a679-3931d2e1c0bb",
        ]

        try:
            return (
                pl.scan_csv(
                    input, dtypes=dtypes.responses, raise_if_empty=False, ignore_errors=True
                )
                .filter(pl.col("class_id").is_not_null())
                .filter(pl.col("student_id").is_not_null())
                .filter(pl.col("response").is_not_null())
                .filter(~pl.col("lrn_question_reference").is_in(exclude))
                .pipe(self.map_multiple_choice)
                .collect()
                .lazy()
            )
        except exceptions.ColumnNotFoundError:
            return pl.DataFrame(schema=dtypes.responses).lazy()

    @staticmethod
    def map_multiple_choice(responses_df: FrameType) -> pl.LazyFrame:
        """Map multiple-choice responses to their corresponding values."""
        df = responses_df.lazy()
        if "lrn_type" not in responses_df.columns or "response" not in responses_df.columns:
            return df

        ref = "lrn_question_reference"
        mcq_responses = (
            df.filter(pl.col("lrn_type") == "mcq")
            .with_columns(
                response=pl.when(pl.col("response").eq(pl.lit("")))
                .then("[]")
                .otherwise(pl.col("response"))
                .str.json_extract()
            )
            .explode("response")
            .with_columns(ref_opt=pl.concat_str(ref, pl.lit("lrn_option_"), "response"))
            .drop("response")
        )

        mcq_map = (
            mcq_responses.unique(ref)
            .melt(ref)
            .with_columns(ref_opt=pl.concat_str(ref, "variable"))
            .filter(pl.col("value").is_not_null())
            .rename({"value": "response"})
        )

        mapped_mcq = (
            mcq_responses.drop(ref).join(mcq_map, on="ref_opt", how="left").select(df.columns)
        )

        return pl.concat([mapped_mcq, df.filter(pl.col("lrn_type") != "mcq")])
