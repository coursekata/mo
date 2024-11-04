from itertools import chain
from pathlib import Path

import polars as pl

from mo.domain.data_format import DataFormat
from mo.domain.data_types import LEGACY_SCHEMAS, SCHEMAS, DataType, LegacyDataType, SchemaDict


class DataParsingService:
    def parse(self, file_path: Path) -> pl.LazyFrame:
        data_type = self.identify_type(file_path)
        if not data_type:
            raise ValueError(f"Could not identify data type for {str(file_path)}")

        data_format = self.identify_format(file_path)
        if not data_format:
            raise ValueError(f"Could not identify data format for {str(file_path)}")

        match data_format:
            case DataFormat.PARQUET:
                return pl.scan_parquet(file_path)
            case DataFormat.CSV:
                schema = self.get_schema(data_type)
                return pl.scan_csv(file_path, schema_overrides=schema)

    def identify_type(self, path: Path | str) -> DataType | LegacyDataType | None:
        for data_type in chain(DataType, LegacyDataType):
            if Path(path).stem.lower() == data_type.value.lower():
                return data_type

    def identify_format(self, path: Path) -> DataFormat | None:
        for data_format in DataFormat:
            if path.suffix.lower() == f".{data_format}".lower():
                return data_format

    def get_schema(self, data_type: DataType | LegacyDataType) -> SchemaDict:
        if schema := (
            SCHEMAS.get(data_type)
            if isinstance(data_type, DataType)
            else LEGACY_SCHEMAS.get(data_type)
        ):
            return schema
        raise ValueError(f"No schema found for data type: {data_type}")
