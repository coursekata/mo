from pathlib import Path

import polars as pl

from mo.application.interfaces.processor import IProcessor


class Check:
    def __init__(
        self,
        manifest_path: Path,
        check_path: Path,
        processor: IProcessor,
        include_active: bool = False,
        include_test: bool = False,
    ) -> None:
        self.manifest_path = manifest_path
        self.check_path = check_path
        self.processor = processor
        self.include_active = include_active
        self.include_test = include_test
        self.include_active = include_active
        self.include_test = include_test

    def execute(self) -> list[str]:
        statuses = ["complete", "active"] if self.include_active else ["complete"]
        class_types = ["real", "test"] if self.include_test else ["real"]
        statuses = ["complete", "active"] if self.include_active else ["complete"]
        class_types = ["real", "test"] if self.include_test else ["real"]
        existing_ids = (
            self.processor.read_output(self.check_path, "parquet").select("class_id").unique()
        )

        manifest_ids = (
            self.processor.read_output(self.manifest_path, "csv")
            .filter(pl.col("class_type").is_in(class_types))  # type: ignore
            .filter(pl.col("status").is_in(statuses))  # type: ignore
            .select("class_id")
            .unique()
        )

        return (
            manifest_ids.join(existing_ids, on="class_id", how="anti")
            .collect()
            .to_series()
            .to_list()
        )
