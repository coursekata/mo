from pathlib import Path

import polars as pl

from mo.application.interfaces.processor import IProcessor


class Check:
    def __init__(self, manifest_path: Path, check_path: Path, processor: IProcessor) -> None:
        self.manifest_path = manifest_path
        self.check_path = check_path
        self.processor = processor

    def execute(self) -> list[str]:
        existing_ids = (
            self.processor.read_output(self.check_path, "parquet").select("class_id").unique()
        )

        manifest_ids = (
            self.processor.read_output(self.manifest_path, "csv")
            .filter(pl.col("class_type") == "real")
            .filter(pl.col("status") == "complete")
            .select("class_id")
            .unique()
        )

        return (
            manifest_ids.join(existing_ids, on="class_id", how="anti")
            .collect()
            .to_series()
            .to_list()
        )
