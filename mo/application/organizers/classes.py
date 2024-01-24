from pathlib import Path
from typing import Collection, final

from mo.application.actions import (
    DeleteFileAction,
    MergeToCSVAction,
    MergeToParquetAction,
    RemoveEmptyDirectoryAction,
)
from mo.application.constants import DataType
from mo.application.organizers.base import BaseDataFileOrganizer
from mo.application.plan import Plan
from mo.application.processors.classes import ClassesProcessor
from mo.application.utils import is_only_file_in_dir


@final
class ClassesOrganizer(BaseDataFileOrganizer):
    pattern = "classes.csv"
    data_type = DataType.classes
    processor = ClassesProcessor()

    def organize(self, inputs: Collection[Path], output: Path) -> Plan:
        self.log.debug(f"Planning {self.data_type} organization")
        plan = Plan()
        output_file = output / "classes.csv"
        plan.actions.append(MergeToCSVAction(self.processor, self.iter_files(inputs), output_file))
        if self.config.remove:
            for data_file in self.iter_files(inputs):
                plan.actions.append(DeleteFileAction(data_file))
                if data_file.parent != output and is_only_file_in_dir(data_file):
                    plan.actions.append(RemoveEmptyDirectoryAction(data_file.parent))
        return plan

    def consolidate(self, inputs: Collection[Path], output: Path) -> Plan:
        self.log.debug(f"Planning {self.data_type} consolidation")
        plan = Plan()
        output_file = output / "classes.parquet"
        inputs = list(self.iter_files(inputs))

        plan.actions.append(MergeToParquetAction(self.processor, inputs, output_file))

        if self.config.remove:
            for data_file in self.iter_files(inputs):
                plan.actions.append(DeleteFileAction(data_file))
                if data_file.parent != output and is_only_file_in_dir(data_file):
                    plan.actions.append(RemoveEmptyDirectoryAction(data_file.parent))

        return plan
