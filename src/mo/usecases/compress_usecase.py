import tempfile
from collections.abc import Iterable
from pathlib import Path
from typing import final

from pydantic import DirectoryPath

from mo.domain.config import Config
from mo.domain.data_format import DataFormat
from mo.domain.data_types import AnyData, DataType
from mo.domain.file_metadata import FileMetadata
from mo.domain.observer import Observer, ProgressEvent
from mo.domain.plan import Plan, PlannedAction
from mo.services.file_discovery import FileDiscoveryService
from mo.services.parsing import DataParsingService
from mo.services.validation import FastValidationService, ValidationService
from mo.usecases.actions import CopyFile, DeleteFile, MergeFiles, MoveFile
from mo.usecases.usecase import UseCase


class Input(Config):
    inputs: list[DirectoryPath]
    output: Path
    move: bool = False
    skip_validation: bool = False
    dry_run: bool = False


@final
class CompressUseCase(UseCase):
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
        # discover files to process
        discovery_service = FileDiscoveryService(
            self.config.inputs,
            DataParsingService(),
            FastValidationService() if self.config.skip_validation else ValidationService(),
            extraction_directory,
        )
        discovery_service.register(self.observers)

        # organize by type because we will compress each type to a single file
        metadatas = discovery_service.discover()
        metadatas_by_type: dict[AnyData, list[FileMetadata]] = {}
        for metadata in metadatas:
            metadatas_by_type.setdefault(metadata.type, []).append(metadata)

        counts = {str(k): len(v) for k, v in metadatas_by_type.items()}
        self.log.info(f"Found files to compress: {counts}")

        # plan what to do with the files
        plan = Plan(list(self.make_plan_actions(metadatas_by_type)))
        plan.register(self.observers)
        return plan

    def make_plan_actions(
        self, metadatas_by_type: dict[AnyData, list[FileMetadata]]
    ) -> Iterable[PlannedAction]:
        def merge_and_delete(
            data_type: DataType, metadata_list: list[FileMetadata], unique_by: list[str]
        ) -> Iterable[PlannedAction]:
            yield MergeFiles(
                metadata_list,
                self.config.output / f"{data_type.value}.parquet",
                unique_by=unique_by,
                output_format=DataFormat.PARQUET,
            )
            if self.config.move:
                for metadata in metadata_list:
                    yield DeleteFile(metadata)

        for data_type, metadata_list in metadatas_by_type.items():
            if data_type in {DataType.RESPONSES}:
                yield from merge_and_delete(
                    data_type,
                    metadata_list,
                    ["class_id", "student_id", "dt_submitted"],
                )
            elif data_type in {DataType.PAGE_VIEWS}:
                yield from merge_and_delete(
                    data_type,
                    metadata_list,
                    ["class_id", "student_id", "chapter", "page", "dt_accessed"],
                )
            elif data_type in {DataType.MEDIA_VIEWS}:
                yield from merge_and_delete(
                    data_type,
                    metadata_list,
                    ["class_id", "student_id", "chapter", "page", "media_id"],
                )
            elif data_type in {DataType.CLASSES, DataType.MANIFEST}:
                yield from merge_and_delete(
                    data_type,
                    metadata_list,
                    ["class_id"],
                )
            elif data_type == "supplementary":
                supp_dir = self.config.output / "supplementary"
                for metadata in metadata_list:
                    if not metadata.class_id:
                        self.log.warning(f"Skipping file missing class_id: {metadata}")
                        continue
                    output = supp_dir / metadata.class_id / metadata.path.name
                    yield (
                        MoveFile(metadata, output)
                        if self.config.move
                        else CopyFile(metadata, output)
                    )
