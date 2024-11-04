from pathlib import Path
from typing import final

import polars as pl

from mo.domain.config import Config
from mo.domain.data_format import DataFormat
from mo.domain.data_types import DataType
from mo.usecases.usecase import DataReadingUseCase


class Input(Config):
    input: Path
    output: Path


@final
class ProcessResponsesUseCase(DataReadingUseCase):
    Input = Input

    def __init__(self, config: Input) -> None:
        super().__init__()
        self.config = config

    def execute(self) -> None:
        self.log.info("Processing responses data")
        if not self.config.input.exists():
            raise FileNotFoundError(f"Input responses file does not exist: {self.config.input}")

        self.log.info("Cleaning responses data")
        self._write_data(
            self._load_data(DataType.RESPONSES, [self.config.input]).pipe(
                ProcessResponsesUseCase.process
            ),
            self.config.output,
            DataFormat.PARQUET,
        )

    @staticmethod
    def process(df: pl.LazyFrame) -> pl.LazyFrame:
        return (
            df.pipe(ProcessResponsesUseCase._exclude_bad_items)
            .filter(pl.col("class_id").is_not_null())
            .filter(pl.col("student_id").is_not_null())
            .filter(pl.col("response").is_not_null())
            .pipe(ProcessResponsesUseCase._map_multiple_choice)
        )

    @staticmethod
    def _exclude_bad_items(df: pl.LazyFrame) -> pl.LazyFrame:
        exclusions = [
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
        return df.filter(~pl.col("lrn_question_reference").is_in(exclusions))

    @staticmethod
    def _map_multiple_choice(df: pl.LazyFrame) -> pl.LazyFrame:
        """Map multiple-choice responses to their corresponding values."""
        df_schema = df.collect_schema()

        if "lrn_type" not in df_schema or "response" not in df_schema:
            return df

        ref = "lrn_question_reference"
        mcq_responses = (
            df.filter(pl.col("lrn_type") == "mcq")
            .with_columns(
                response=pl.when(pl.col("response").eq(pl.lit("")))
                .then(pl.lit("[]"))
                .otherwise(pl.col("response"))
                .str.json_decode()
            )
            .explode("response")
            .with_columns(ref_opt=pl.concat_str(ref, pl.lit("lrn_option_"), "response"))
            .drop("response")
        )

        mcq_map = (
            mcq_responses.unique(ref)
            .unpivot(index=ref)
            .with_columns(ref_opt=pl.concat_str(ref, "variable"))
            .filter(pl.col("value").is_not_null())
            .rename({"value": "response"})
        )

        mapped_mcq = (
            mcq_responses.drop(ref)
            .join(mcq_map, on="ref_opt", how="left")
            .select(list(df_schema.keys()))
        )

        return pl.concat([mapped_mcq, df.filter(pl.col("lrn_type") != "mcq")])
