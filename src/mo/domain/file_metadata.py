from functools import cached_property
from pathlib import Path
from typing import Literal

from pydantic import BaseModel

from mo.domain.data_types import DataType, LegacyDataType


class FileMetadata(BaseModel):
    path: Path
    type: DataType | LegacyDataType | Literal["supplementary"]
    class_id: str | None = None

    @cached_property
    def name(self) -> str:
        return str(self.path)


class ZipFileMetadata(FileMetadata):
    member_path: str
    archive_path: Path

    @cached_property
    def name(self) -> str:
        return f"{self.archive_path}::{self.member_path}"
