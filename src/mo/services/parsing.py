from itertools import chain
from pathlib import Path

import polars as pl

from mo.domain.data_types import LEGACY_SCHEMAS, SCHEMAS, DataType, LegacyDataType, SchemaDict


class DataParsingService:
    def identify_type(self, filename: str) -> DataType | LegacyDataType | None:
        for data_type in chain(DataType, LegacyDataType):
            if f"{data_type.value}.csv" in filename:
                return data_type

    def parse(self, file_path: Path) -> pl.LazyFrame:
        data_type = self.identify_type(file_path.name)
        if not data_type:
            raise ValueError(f"Could not identify data type for file: {file_path}")
        schema = self.get_schema(data_type)
        return pl.scan_csv(file_path, schema_overrides=schema)

    def get_schema(self, data_type: DataType | LegacyDataType) -> SchemaDict:
        if schema := (
            SCHEMAS.get(data_type)
            if isinstance(data_type, DataType)
            else LEGACY_SCHEMAS.get(data_type)
        ):
            return schema
        raise ValueError(f"No schema found for data type: {data_type}")
