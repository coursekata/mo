from typing import final

from mo.application.interfaces.organizer import DataType
from mo.application.organizers.base import ClassDataFileOrganizer
from mo.application.processors.media_views import MediaViewsProcessor


@final
class MediaViewsOrganizer(ClassDataFileOrganizer):
    data_type = DataType.media_views
    pattern = "media_views.csv"
    processor = MediaViewsProcessor()
