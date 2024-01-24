from pathlib import Path
from typing import Collection, final
from uuid import UUID

from pydantic import UUID4

from mo.application.interfaces.organizer import DataType
from mo.application.organizers.base import ClassFileOrganizer
from mo.application.plan import Plan


@final
class SupplementaryOrganizer(ClassFileOrganizer):
    data_type = DataType.supplementary
    pattern = "supplementary/*"

    def make_organized_path(self, output: Path, class_id: UUID, data_file: Path) -> Path:
        return output / str(class_id) / "supplementary" / data_file.name

    def make_consolidated_path(self, output: Path, class_id: UUID, data_file: Path) -> Path:
        return output / "supplementary" / str(class_id) / data_file.name

    def deduplicate(
        self, inputs: Collection[Path]
    ) -> tuple[dict[UUID4, dict[str, Path]], set[Path]]:
        organized, processed = super().deduplicate(inputs)
        for name_path in organized.values():
            if ".keep" in name_path:
                del name_path[".keep"]
        return organized, processed

    def get_class_id(self, input: Path) -> UUID4 | None:
        # path should be one of these:
        # /somewhere/on/system/[UUID]/supplementary/[something]
        # /somewhere/on/system/supplementary/[UUID]/[something]
        for directory in [input.parent, input.parent.parent]:
            try:
                return UUID(directory.name)
            except ValueError:
                continue
        self.log.warning(f"Could not get class ID from {input}")
        return None

    def consolidate(self, inputs: Collection[Path], output: Path) -> Plan:
        return self.organize(inputs, output, self.make_consolidated_path)
