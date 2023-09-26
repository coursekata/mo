"""Check that data has been downloaded for all completed classes in the manifest."""


import polars as pl

from mo.core.interfaces import IReader
from mo.core.read.classes import ManifestReader
from mo.core.typing import PathStr


class ManifestChecker:
    manifest_reader: IReader = ManifestReader()

    def __call__(self, manifest_path: PathStr, organized_path: PathStr) -> list[str]:
        """Check download folder for all completed classes in the manifest."""
        organized_ids = pl.scan_parquet(organized_path).select("class_id").unique()

        manifest_ids = (
            self.manifest_reader(manifest_path)
            .filter(pl.col("class_type") == "real")
            .filter(pl.col("status") == "complete")
            .select("class_id")
            .unique()
        )

        return (
            manifest_ids.join(organized_ids, on="class_id", how="anti")
            .collect()
            .to_series()
            .to_list()
        )
