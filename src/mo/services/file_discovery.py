import itertools
import zipfile
from collections.abc import Iterable
from pathlib import Path
from typing import TypeVar, final

from mo.domain.data_types import DataType
from mo.domain.file_metadata import FileMetadata, ZipFileMetadata
from mo.domain.observer import Observable, ProgressEvent
from mo.services.parsing import DataParsingService
from mo.services.validation import ValidationService

FileMetadataType = TypeVar("FileMetadataType", bound=FileMetadata)


@final
class FileDiscoveryService(Observable[ProgressEvent]):
    def __init__(
        self,
        dirs: Iterable[Path],
        parser_svc: DataParsingService,
        validation_svc: ValidationService,
        extraction_dir: Path,
    ) -> None:
        super().__init__()
        self.dirs = list(dirs)
        self.parser_svc = parser_svc
        self.validation_svc = validation_svc
        self.extraction_dir = extraction_dir

    def discover(self) -> Iterable[FileMetadata]:
        targets = list(itertools.chain.from_iterable(dir.rglob("*") for dir in self.dirs))
        zip_targets = list(itertools.chain.from_iterable(dir.rglob("*.zip") for dir in self.dirs))
        total_targets = len(targets) + len(zip_targets)

        progress = ProgressEvent(current=0, total=total_targets, message="Discovering files")
        self.notify(progress)

        metadatas: list[FileMetadata] = []
        supplementary_dirs: list[FileMetadata] = []
        for path in targets:
            self.notify(progress.advance())
            if path.is_dir() and path.name == "supplementary":
                # we have to wait until we have all the file metadata to properly evaluate these
                supplementary_dirs.append(FileMetadata(path=path, type="supplementary"))
            elif path.is_file() and (processed := self.process_data_file(path)):
                metadatas.append(processed)

        extracted_files: list[FileMetadata] = []
        for path in zip_targets:
            self.notify(progress.advance())
            extracted_files.extend(self.process_zip_file(path))

        return self.remove_duplicates_and_unidentifiables(
            itertools.chain(
                metadatas,
                self.process_supplementary(supplementary_dirs, metadatas),
                extracted_files,
            )
        )

    def remove_duplicates_and_unidentifiables(
        self, metadatas: Iterable[FileMetadata]
    ) -> Iterable[FileMetadata]:
        seen: set[tuple[str, str]] = set()
        for metadata in metadatas:
            if metadata.type in (DataType.CLASSES, DataType.MANIFEST):
                # these have many class IDs not one, so we don't filter them
                yield metadata
                continue

            if not metadata.class_id:
                # everything else must belong to a class
                continue

            key = (str(metadata.type), metadata.class_id)
            if key not in seen:
                seen.add(key)
                yield metadata

    def process_zip_file(self, path: Path) -> Iterable[ZipFileMetadata]:
        metadatas: list[ZipFileMetadata] = []
        supplementary_dirs: list[ZipFileMetadata] = []
        with zipfile.ZipFile(path, "r") as zip_file:
            for name in zip_file.namelist():
                if name.endswith("supplementary/"):
                    zip_file.extract(name, self.extraction_dir)
                    extracted_path = self.extraction_dir / name
                    metadata = ZipFileMetadata(
                        path=extracted_path,
                        type="supplementary",
                        class_id=extracted_path.parent.name,
                        archive_path=path.parent,
                        member_path=name,
                    )
                    # we have to wait until we have all the file metadata to properly evaluate these
                    supplementary_dirs.append(metadata)
                elif self.parser_svc.identify_type(name):
                    zip_file.extract(name, self.extraction_dir)
                    extracted_path = self.extraction_dir / name
                    if processed := self.process_data_file(extracted_path):
                        metadata = ZipFileMetadata(
                            **dict(processed),
                            archive_path=path.parent,
                            member_path=name,
                        )
                        metadatas.append(metadata)

        return itertools.chain(metadatas, self.process_supplementary(supplementary_dirs, metadatas))

    def process_data_file(self, path: Path) -> FileMetadata | None:
        if data_type := self.parser_svc.identify_type(path):
            strategy = self.validation_svc.get_strategy(data_type)
            is_valid, class_id = strategy.validate(path)
            if is_valid:
                return FileMetadata(path=path, type=data_type, class_id=class_id)

    def process_supplementary(
        self,
        supplementary_dirs: list[FileMetadataType],
        metadatas: list[FileMetadataType],
    ) -> Iterable[FileMetadataType]:
        # a valid supplementary directory must be a direct child of a directory containing data
        # files. so, we start by organizing the supplementary directories by their parent directory
        supplementary_by_parent = {
            metadata.path.parent: metadata for metadata in supplementary_dirs
        }

        # then we organize the data files by their parent directory
        metadata_by_parent: dict[Path, list[FileMetadataType]] = {}
        for metadata in metadatas:
            parent = metadata.path.parent
            if parent in supplementary_by_parent:
                metadata_by_parent.setdefault(parent, []).append(metadata)

        # now we can evaluate whether the supplementary directories have any data files as siblings
        for supplementary in supplementary_dirs:
            siblings = metadata_by_parent.get(supplementary.path, [])
            if any(sibling.type in DataType for sibling in siblings) and all(
                sibling.class_id == supplementary.class_id for sibling in siblings
            ):
                yield supplementary
