import tempfile
from collections.abc import Iterable
from pathlib import Path
from typing import final

from pydantic import DirectoryPath

from mo.domain.config import Config
from mo.domain.data_types import DataType, LegacyDataType
from mo.domain.file_metadata import FileMetadata
from mo.domain.observer import Observer, ProgressEvent
from mo.domain.plan import Plan, PlannedAction
from mo.services.file_discovery import FileDiscoveryService
from mo.services.parsing import DataParsingService
from mo.services.validation import ValidationService
from mo.usecases.actions import CopyFile, DeleteFile, IgnoreLegacyFile, MergeFiles, MoveFile
from mo.usecases.usecase import UseCase


class Input(Config):
    inputs: list[DirectoryPath]
    output: Path
    move: bool = True
    ignore_legacy: bool = False
    ignore_duplicates: bool = False
    dry_run: bool = False


@final
class OrganizeUseCase(UseCase):
    Input = Input

    def __init__(
        self, config: Input, observers: Iterable[Observer[ProgressEvent]] | None = None
    ) -> None:
        super().__init__()
        self.config = config
        self.observers = observers or []

    def execute(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            plan = self.prepare_plan(Path(temp_dir))
            if self.config.dry_run:
                plan.describe()
            else:
                plan.execute()

    def prepare_plan(self, extraction_directory: Path) -> Plan:
        self.log.info(f"Planning how to organize into {str(self.config.output)}")

        # discover files to process
        discovery_service = FileDiscoveryService(
            self.config.inputs,
            DataParsingService(),
            ValidationService(),
            extraction_directory,
        )
        discovery_service.register(self.observers)
        file_metadata_list = discovery_service.discover()

        # plan what to do with the files
        plan = Plan(list(self.make_plan_actions(file_metadata_list)))
        plan.register(self.observers)
        return plan

    def make_plan_actions(
        self, file_metadata_list: Iterable[FileMetadata]
    ) -> Iterable[PlannedAction]:
        manifests: list[FileMetadata] = []
        classes: list[FileMetadata] = []

        # make actions for data files
        for metadata in file_metadata_list:
            if metadata.type in LegacyDataType:
                yield (
                    IgnoreLegacyFile(metadata)
                    if self.config.ignore_legacy
                    else DeleteFile(metadata)
                )
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
                    if self.config.move
                    else CopyFile(metadata, output)
                )

        # merge manifests and classes into single files
        for lst in [manifests, classes]:
            if not lst:
                continue
            output = self.config.output / lst[0].path.name
            if len(lst) == 1:
                yield MoveFile(lst[0], output) if self.config.move else CopyFile(lst[0], output)
            elif len(lst) > 1:
                yield MergeFiles(lst, output, "class_id")
