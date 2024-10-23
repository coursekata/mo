from typing import final

from mo.application.constants import DataType
from mo.application.organizers.base import DataDeleterOrganizer


@final
class TagsOrganizer(DataDeleterOrganizer):
    pattern = "tags.csv"
    data_type = DataType.tags
