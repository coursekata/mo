import logging
import shutil
from collections.abc import Iterable
from pathlib import Path

import polars as pl

from mo.domain.config import Config
from mo.domain.data_types import DataType, LegacyDataType
from mo.domain.file_metadata import FileMetadata
from mo.domain.plan import PlannedAction
from mo.services.parsing import DataParsingService


class ActionFactoryConfig(Config):
    output: Path
    move: bool = True
    ignore_legacy: bool = False
    ignore_duplicates: bool = False


class ActionFactory:
    def __init__(self, config: ActionFactoryConfig) -> None:
        self.config = config
        self.log = logging.getLogger(__name__)

    def make(self, file_metadata_list: Iterable[FileMetadata]) -> Iterable[PlannedAction]:
        move, ignore_legacy = self.config.move, self.config.ignore_legacy

        manifests: list[FileMetadata] = []
        classes: list[FileMetadata] = []

        for metadata in file_metadata_list:
            if metadata.type in LegacyDataType:
                yield IgnoreLegacyFile(metadata) if ignore_legacy else DeleteFile(metadata)
            elif metadata.type == DataType.CLASSES:
                manifests.append(metadata)
            elif metadata.type == DataType.MANIFEST:
                classes.append(metadata)
            elif metadata.type in DataType or metadata.type == "supplementary":
                if not metadata.class_id:
                    self.log.warning(f"File {metadata.name} has no class ID and will be ignored.")
                    continue
                output = self.config.output / metadata.class_id / metadata.path.name
                yield (
                    MoveFile(metadata, output, self.config.ignore_duplicates)
                    if move
                    else CopyFile(metadata, output)
                )

        # merge manifests and classes into single files
        for lst in [manifests, classes]:
            if not lst:
                continue
            output = self.config.output / lst[0].path.name
            if output.exists():
                lst.append(FileMetadata(path=output, type=lst[0].type))
            if len(lst) == 1:
                yield MoveFile(lst[0], output) if move else CopyFile(lst[0], output)
            elif len(lst) > 1:
                yield MergeFiles(lst, output, "class_id")


class MergeFiles(PlannedAction):
    def __init__(
        self,
        metadatas: list[FileMetadata],
        output_path: Path,
        unique_by: str | list[str] | None = None,
        parser: DataParsingService | None = None,
    ) -> None:
        self.metadatas = metadatas
        self.output_path = output_path
        self.parser = parser or DataParsingService()
        self.unique_by = unique_by

    def execute(self) -> None:
        (
            pl.concat(
                [self.parser.parse(metadata.path) for metadata in self.metadatas],
                how="diagonal_relaxed",
            )
            .unique(self.unique_by)
            .collect(streaming=True)
            .write_csv(self.output_path)
        )

    def describe(self) -> str:
        files = ", ".join(metadata.name for metadata in self.metadatas)
        return f"Merging multiple manifest files to {str(self.output_path)}: {files}"


class FileActionBase(PlannedAction):
    def _move(self, src: Path, dst: Path) -> None:
        shutil.move(src, dst)

    def _copy(self, src: Path, dst: Path) -> None:
        if src.is_dir():
            shutil.copytree(src, dst)
        else:
            shutil.copy(src, dst)

    def _remove(self, path: Path) -> None:
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()


class MoveCopyBase(FileActionBase):
    def __init__(
        self,
        metadata: FileMetadata,
        output_path: Path,
        ignore_duplicates: bool = False,
    ) -> None:
        self.metadata = metadata
        self.output_path = output_path
        self.ignore_duplicates = ignore_duplicates

    def _output_is_newer(self) -> bool:
        return (
            self.output_path.exists()
            and self.metadata.path.stat().st_mtime <= self.output_path.stat().st_mtime
        )


class MoveFile(MoveCopyBase):
    def execute(self) -> None:
        if self._output_is_newer() and not self.ignore_duplicates:
            self._remove(self.metadata.path)
        else:
            self.output_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(self.metadata.path, self.output_path)

    def describe(self) -> str:
        if self._output_is_newer():
            return f"Skipping older {self.metadata.name}"
        return f"Moving {self.metadata.name} to {str(self.output_path)}"


class CopyFile(MoveCopyBase):
    def execute(self) -> None:
        if not self._output_is_newer():
            self.output_path.parent.mkdir(parents=True, exist_ok=True)
            self._copy(self.metadata.path, self.output_path)

    def describe(self) -> str:
        if self._output_is_newer():
            return f"Skipping older {self.metadata.name}"
        return f"Copying {self.metadata.name} to {str(self.output_path)}"


class DeleteFile(FileActionBase):
    def __init__(self, metadata: FileMetadata) -> None:
        self.metadata = metadata

    def execute(self) -> None:
        self._remove(self.metadata.path)

    def describe(self) -> str:
        return f"Deleting {self.metadata.name}"


class IgnoreLegacyFile(PlannedAction):
    def __init__(self, metadata: FileMetadata) -> None:
        self.metadata = metadata

    def execute(self) -> None:
        pass

    def describe(self) -> str:
        return f"Ignoring legacy file {self.metadata.name}"
