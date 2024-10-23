from enum import Enum


class DataType(Enum):
    """The different types of data that can be organized."""

    classes = "classes"
    responses = "responses"
    media_views = "media_views"
    page_views = "page_views"
    supplementary = "supplementary"
    tags = "tags"
    items = "items"


NULL_VALUES = [""]
"""Strings that are considered as null values in the data."""
