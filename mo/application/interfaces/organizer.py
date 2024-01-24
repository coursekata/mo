from abc import abstractmethod
from pathlib import Path
from typing import Collection

from mo.application.constants import DataType
from mo.application.plan import Plan


class IOrganizer:
    pattern: str | Collection[str]
    """The pattern to use to find the data files."""

    data_type: DataType
    """The type of data being organized."""

    @abstractmethod
    def organize(self, inputs: Collection[Path], output: Path) -> Plan:
        """Plan how to organize the data."""

    @abstractmethod
    def consolidate(self, inputs: Collection[Path], output: Path) -> Plan:
        """Plan how to consolidate the data."""
