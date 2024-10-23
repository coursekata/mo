from typing import final

from mo.application.constants import DataType
from mo.application.organizers.base import DataDeleterOrganizer


@final
class ItemsOrganizer(DataDeleterOrganizer):
    pattern = "items.csv"
    data_type = DataType.items
