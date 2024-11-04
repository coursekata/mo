import shutil
from pathlib import Path

import polars as pl

from mo.domain.data_format import DataFormat
from mo.domain.file_metadata import FileMetadata
from mo.domain.plan import PlannedAction
from mo.services.parsing import DataParsingService


class MergeFiles(PlannedAction):
    def __init__(
        self,
        metadatas: list[FileMetadata],
        output_path: Path,
        unique_by: str | list[str] | None = None,
        parser: DataParsingService | None = None,
        output_format: DataFormat = DataFormat.CSV,
    ) -> None:
        self.metadatas = metadatas
        self.output_path = output_path
        self.parser = parser or DataParsingService()
        self.unique_by = unique_by
        self.output_format = output_format

    def execute(self) -> None:
        self.output_path.parent.mkdir(parents=True, exist_ok=True)

        dfs = [self.parser.parse(metadata.path) for metadata in self.metadatas]
        if self.output_path.exists():
            dfs.append(self.parser.parse(self.output_path))

        df = pl.concat(dfs, how="diagonal_relaxed").unique(self.unique_by)
        if self.output_format == DataFormat.CSV:
            df.collect(streaming=True).write_csv(self.output_path)
        elif self.output_format == DataFormat.PARQUET:
            df.collect(streaming=True).write_parquet(self.output_path)
        else:
            raise ValueError(f"Unsupported output format: {self.output_format}")

    def describe(self) -> str:
        return f"Merging {len(self.metadatas)} files to {str(self.output_path)}"


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
