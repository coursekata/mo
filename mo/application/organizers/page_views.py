from typing import final

from mo.application.interfaces.organizer import DataType
from mo.application.organizers.base import ClassDataFileOrganizer
from mo.application.processors.page_views import PageViewsProcessor


@final
class PageViewsOrganizer(ClassDataFileOrganizer):
    data_type = DataType.page_views
    pattern = "page_views.csv"
    processor = PageViewsProcessor()
