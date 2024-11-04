import logging
from abc import ABC, abstractmethod
from collections.abc import Iterable
from functools import reduce
from pathlib import Path
from typing import Any
from zipfile import ZipFile

import polars as pl

from mo.domain.data_format import DataFormat
from mo.domain.data_types import SCHEMAS, DataType, SchemaDict


class UseCase(ABC):
    def __init__(self):
        self.log = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def execute(self) -> Any:
        """Executes the use case."""


class DataReadingUseCase(UseCase):
    def _find_data(
        self, input_dirs: list[Path], data_type: DataType, formats: list[DataFormat]
    ) -> list[Path]:
        seen: dict[Path, Path] = {}
        dtype_targets = self._targets([data_type], formats)
        for input_dir in input_dirs:
            for pattern in dtype_targets:
                for target in input_dir.rglob(pattern):
                    if (
                        target.parent not in seen
                        or target.stat().st_mtime > seen[target.parent].stat().st_mtime
                    ):
                        seen[target.parent] = target
        return list(seen.values())

    def _targets(self, dtypes: list[DataType], formats: list[DataFormat]) -> list[str]:
        return [f"{dtype.value}.{format.value}" for dtype in dtypes for format in formats]

    def _unpack_targets(self, zip_file: Path, targets: list[str]) -> None:
        self.log.debug(f"Unpacking {str(zip_file)} for {targets}")
        with ZipFile(zip_file, "r") as zip:
            for file in zip.namelist():
                for target in targets:
                    if file.endswith(target) and not Path(zip_file.parent, file).exists():
                        self.log.debug(f"Extracting {str(file)}")
                        zip.extract(file, zip_file.parent)

    def _load_data(self, dtype: DataType, inputs: list[Path]) -> pl.LazyFrame:
        self.log.debug(f"Loading {dtype} data from {[str(i) for i in inputs]}")
        return self._concat_data(self._read_data(input, dtype) for input in inputs)

    def _concat_data(self, dfs: Iterable[pl.LazyFrame]) -> pl.LazyFrame:
        return reduce(lambda x, y: pl.concat([x, y], how="diagonal_relaxed"), dfs)

    def _read_data(self, input: Path, dtype: DataType) -> pl.LazyFrame:
        self.log.debug(f"Reading {str(input)}")
        format = DataFormat(input.suffix[1:])
        if format is DataFormat.CSV:
            return self._read_csv(input, SCHEMAS.get(dtype, {}))
        elif format is DataFormat.PARQUET:
            return self._read_parquet(input)
        raise ValueError(f"Unsupported data format: {format}")

    def _read_csv(self, input: Path, schema: SchemaDict) -> pl.LazyFrame:
        try:
            input_schema = pl.scan_csv(input, infer_schema_length=0).collect_schema()
            return (
                pl.scan_csv(input, schema_overrides=schema, try_parse_dates=True)
                .select([key for key in schema if key in input_schema])
                .unique()
            )

        except pl.exceptions.NoDataError as exc:
            self.log.debug(f"Skipped {str(input)}: {exc}")
            return pl.LazyFrame(schema=schema)

    def _read_parquet(self, input: Path) -> pl.LazyFrame:
        return pl.scan_parquet(input).unique()

    def _write_data(self, data: pl.LazyFrame, output_path: Path, output_format: DataFormat) -> None:
        self.log.debug(f"Writing data to {str(output_path)}")
        if output_format is DataFormat.PARQUET:
            data.collect(streaming=True).write_parquet(output_path)
        elif output_format is DataFormat.CSV:
            data.collect(streaming=True).write_csv(output_path)
