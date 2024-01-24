import shutil
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Iterable, final

from mo.application.interfaces.processor import IProcessor


class IAction(ABC):
    @abstractmethod
    def __str__(self) -> str:
        pass

    @abstractmethod
    def execute(self) -> None:
        pass


class BaseAction(IAction):
    def make_str(self, *args: Any) -> str:
        return "\n  ".join(str(arg) for arg in (self.__class__.__name__, *args))

    def __str__(self) -> str:
        return self.make_str()


class PathAction(BaseAction):
    def __init__(self, path: Path):
        self.path = path

    def __str__(self) -> str:
        return self.make_str(self.path)


class SrcDestAction(BaseAction):
    def __init__(self, source: Path, destination: Path):
        self.source = source
        self.destination = destination

    def __str__(self) -> str:
        src = f"src: {self.source}"
        dest = f"dst: {self.destination}"
        return self.make_str(src, dest)


class MergeAction(BaseAction):
    def __init__(self, processor: IProcessor, source: Iterable[Path], destination: Path):
        self.source = source
        self.destination = destination
        self.processor = processor

    def __str__(self) -> str:
        self.source = tuple(self.source)
        srcs = (f"src: {src}" for src in self.source)
        dest = f"dst: {self.destination}"
        return self.make_str(*srcs, dest)


@final
class MakeDirectoryAction(PathAction):
    def execute(self) -> None:
        self.path.mkdir(parents=True, exist_ok=True)


@final
class RemoveEmptyDirectoryAction(PathAction):
    def execute(self) -> None:
        if self.path.is_dir() and not any(self.path.iterdir()):
            self.path.rmdir()


@final
class MergeToCSVAction(MergeAction):
    def execute(self) -> None:
        self.processor.raw_merge(self.source, self.destination, "csv")


@final
class MergeToParquetAction(MergeAction):
    def execute(self) -> None:
        self.processor.merge(self.source, self.destination, "parquet")


@final
class MergeParquetAction(MergeAction):
    def execute(self) -> None:
        dfs = (self.processor.read_output(path, "parquet") for path in self.source)
        self.processor.write(self.processor.concat(dfs), self.destination, "parquet")


@final
class DeleteFileAction(PathAction):
    def execute(self) -> None:
        if self.path.exists():
            self.path.unlink()


@final
class MoveFileAction(SrcDestAction):
    def execute(self) -> None:
        self.destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(self.source, self.destination)


@final
class CopyFileAction(SrcDestAction):
    def execute(self) -> None:
        self.destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(self.source, self.destination)
