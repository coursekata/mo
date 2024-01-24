import logging
from abc import abstractmethod
from collections import defaultdict
from pathlib import Path
from typing import Callable, Collection, Iterable
from uuid import UUID

from pydantic import UUID4

from mo.application.actions import (
    CopyFileAction,
    DeleteFileAction,
    MergeParquetAction,
    MergeToParquetAction,
    MoveFileAction,
    RemoveEmptyDirectoryAction,
)
from mo.application.interfaces.organizer import IOrganizer
from mo.application.interfaces.processor import IProcessor
from mo.application.plan import Plan
from mo.application.use_cases.organize import OrganizeConfig
from mo.application.utils import is_only_file_in_dir


class BaseOrganizer(IOrganizer):
    def __init__(self, config: OrganizeConfig, pattern: str | Collection[str] | None = None):
        self.config = config
        self.log = logging.getLogger(self.__class__.__name__)
        if pattern:
            self.pattern = pattern
        if not self.pattern or not self.data_type:
            raise ValueError("`pattern` and `data_type` must be set")

    def iter_files(self, inputs: Collection[Path]) -> Iterable[Path]:
        """Iterate over the files to be organized."""
        for i in inputs:
            for p in self.pattern if not isinstance(self.pattern, str) else [self.pattern]:
                yield from i.rglob(p)


class ClassFileOrganizer(BaseOrganizer):
    @abstractmethod
    def get_class_id(self, input: Path) -> UUID4 | None:
        """Get the class ID for the data file."""

    def organize(
        self,
        inputs: Collection[Path],
        output: Path,
        path_factory: Callable[[Path, UUID, Path], Path] | None = None,
    ) -> Plan:
        self.log.debug(f"Planning {self.data_type} organization")
        path_factory = path_factory or self.make_organized_path

        plan = Plan()
        organized, processed = self.deduplicate(inputs)
        for class_id, name_path in organized.items():
            for data_file in name_path.values():
                output_file = path_factory(output, class_id, data_file)
                action = MoveFileAction if self.config.remove else CopyFileAction
                plan.actions.append(action(data_file, output_file))
        if self.config.remove:
            organized_paths = set(p for class_id in organized.values() for p in class_id.values())
            for data_file in processed - organized_paths:
                plan.actions.append(DeleteFileAction(data_file))
                if data_file.parent != output and is_only_file_in_dir(data_file):
                    plan.actions.append(RemoveEmptyDirectoryAction(data_file.parent))
        return plan

    def make_organized_path(self, output: Path, class_id: UUID, data_file: Path) -> Path:
        return output / str(class_id) / data_file.name

    def deduplicate(
        self, inputs: Collection[Path]
    ) -> tuple[dict[UUID4, dict[str, Path]], set[Path]]:
        processed_files: set[Path] = set()
        organized_files: dict[UUID4, dict[str, Path]] = defaultdict(dict)
        for data_file in self.iter_files(inputs):
            self.log.debug(f"Organizing {data_file}")
            class_id = self.get_class_id(data_file)
            if not class_id:
                self.log.warning(f"Could not determine class ID for {data_file}, skipping")
                continue

            processed_files.add(data_file)
            if class_id not in organized_files or data_file.name not in organized_files[class_id]:
                self.log.debug(f"Adding {self.data_type} file for {class_id}: {data_file}")
                organized_files[class_id][data_file.name] = data_file
            else:
                self.log.warning(f"Found existing {self.data_type} file for {class_id}")
                existing = organized_files[class_id][data_file.name]
                self.resolve_conflict(existing, data_file)

        return organized_files, processed_files

    def resolve_conflict(self, existing: Path, new: Path) -> Path:
        """Resolve a conflict between two files."""
        if existing.stat().st_mtime > new.stat().st_mtime:
            self.log.warning(f"Skipping older {new}")
            return existing
        self.log.warning(f"Using newer {new}")
        return new


class BaseDataFileOrganizer(BaseOrganizer):
    processor: IProcessor

    def __init__(self, config: OrganizeConfig, pattern: str | Collection[str] | None = None):
        super().__init__(config, pattern)
        if not self.processor:
            raise ValueError("`pattern`, `data_type`, and `reader` must be set")


class ClassDataFileOrganizer(ClassFileOrganizer, BaseDataFileOrganizer):
    def get_class_id(self, input: Path) -> UUID4 | None:
        """Get the class ID from the data file."""
        class_ids = self.processor.raw_read(input).select("class_id").unique().collect().to_series()
        if len(class_ids) > 1:
            self.log.warning(f"Found multiple class IDs in {input}: {class_ids}")
            return None
        if len(class_ids) < 1:
            self.log.warning(f"Found no class IDs in {input}")
            return None
        return UUID(class_ids[0])

    def consolidate(self, inputs: Collection[Path], output: Path) -> Plan:
        self.log.debug(f"Planning {self.data_type} consolidation")
        plan = Plan()
        output_file = output / f"{self.data_type}.parquet"
        organized, processed = self.deduplicate(inputs)

        # if the output file exists, check that it can be merged with the input files
        # it should be if it can be read with the same schema as the processor's output schema
        if output_file.exists():
            self.log.debug(f"Checking if {output_file} can be merged with inputs")
            try:
                self.processor.read_output(output_file, "parquet")
            except Exception as e:
                raise Exception(f"Output file {output_file} not compatible with processor") from e

        # create a temporary file to hold the merged data, and add it to the cleanup list
        temp_file = output_file.with_suffix(".tmp.parquet")
        # add to cleanup as a failsafe, but should be cleaned up earlier (added below)
        plan.cleanup.append(DeleteFileAction(temp_file))

        # merge all the CSV files to a single Parquet file
        iter_organized = (p for class_id in organized.values() for p in class_id.values())
        plan.actions.append(MergeToParquetAction(self.processor, iter_organized, temp_file))

        if output_file.exists():
            # merge the Parquet file with the output file
            iter_parquet = (temp_file, output_file)
            plan.actions.append(MergeParquetAction(self.processor, iter_parquet, output_file))
        else:
            # move the Parquet file to the output file
            plan.actions.append(MoveFileAction(temp_file, output_file))

        plan.actions.append(DeleteFileAction(temp_file))

        if self.config.remove:
            for data_file in processed:
                plan.actions.append(DeleteFileAction(data_file))
                if data_file.parent != output and is_only_file_in_dir(data_file):
                    plan.actions.append(RemoveEmptyDirectoryAction(data_file.parent))

        return plan


class DataDeleterOrganizer(BaseOrganizer):
    def organize(self, inputs: Collection[Path], output: Path) -> Plan:
        self.log.debug(f"Planning {self.data_type} organization")
        plan = Plan()
        if self.config.remove:
            for data_file in self.iter_files(inputs):
                plan.actions.append(DeleteFileAction(data_file))
        return plan

    def consolidate(self, inputs: Collection[Path], output: Path) -> Plan:
        return self.organize(inputs, output)
