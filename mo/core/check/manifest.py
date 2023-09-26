"""Check that data has been downloaded for all completed classes in the manifest."""

from typing import Any

from mo.core.interfaces import IReader
from mo.core.read.classes import ManifestReader


class ManifestChecker:
    manifest_reader: IReader = ManifestReader()

    def __call__(self, manifest_path: Path, data_dir: Path) -> Any:
        """Check download folder for all completed classes in the manifest."""
