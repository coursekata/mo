"""Custom types (these are not models, these are specifically just types)."""

from enum import StrEnum
from pathlib import Path

PathStr = str | Path


class InstitutionType(StrEnum):
    """Enumeration of the types of institutions."""

    higher_ed = "higher_ed"
    k12 = "k12"
    other = "other"
