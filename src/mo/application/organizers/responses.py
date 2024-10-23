from collections.abc import Callable, Collection
from pathlib import Path
from typing import final
from uuid import UUID

from mo.application.interfaces.organizer import DataType
from mo.application.organizers.base import ClassDataFileOrganizer
from mo.application.plan import Plan
from mo.application.processors.responses import ResponsesProcessor


@final
class ResponsesOrganizer(ClassDataFileOrganizer):
    data_type = DataType.responses
    pattern = "responses.csv"
    processor = ResponsesProcessor()

    def organize(
        self,
        inputs: Collection[Path],
        output: Path,
        path_factory: Callable[[Path, UUID, Path], Path] | None = None,
    ) -> Plan:
        if not self.config.dry_run:
            for data_file in self.iter_files(inputs):
                self.processor.raw_read(data_file).collect().write_csv(data_file)
        return super().organize(inputs, output, path_factory)

    def consolidate(self, inputs: Collection[Path], output: Path) -> Plan:
        if not self.config.dry_run:
            for data_file in self.iter_files(inputs):
                self.processor.raw_read(data_file).collect().write_csv(data_file)
        return super().consolidate(inputs, output)
