"""Custom errors for the metrics package."""

from mo.core.typing import PathStr


class ManifestNotFoundError(FileNotFoundError):
    """Raised when a manifest file is missing."""

    def __init__(self, path: PathStr):
        super().__init__(f"Manifest file not found at {path}")


class EmptyManifestError(Exception):
    """Exception raised when a manifest is empty."""

    def __init__(self, manifest_file: PathStr):
        """Initialize the exception."""
        super().__init__(f"Manifest is empty: {manifest_file}")


class MetadataNotFoundError(FileNotFoundError):
    """Raised when a classes.csv metadata file is missing."""

    def __init__(self, path: PathStr):
        super().__init__(f"Metadata file not found at {path}")
