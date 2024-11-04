from enum import StrEnum
from typing import TypeAlias

import polars as pl
from polars.datatypes import DataTypeClass

SchemaDict: TypeAlias = dict[str, pl.DataType | DataTypeClass]


class DataType(StrEnum):
    MANIFEST = "manifest"
    CLASSES = "classes"
    RESPONSES = "responses"
    MEDIA_VIEWS = "media_views"
    PAGE_VIEWS = "page_views"


SCHEMAS: dict[DataType, SchemaDict] = {
    DataType.CLASSES: {
        "institution_id": pl.Utf8,
        "class_id": pl.Utf8,
        "course_name": pl.Utf8,  # course
        "release": pl.Utf8,  # version
        "book": pl.Utf8,
        "teacher_id": pl.Utf8,  # instructor_id
        "lms": pl.Utf8,
        "setup_yaml": pl.Utf8,
    },
    DataType.MANIFEST: {
        "class_id": pl.Utf8,
        "department": pl.Utf8,
        "type": pl.Utf8,
        "institution_funding": pl.Utf8,
        "institution_level": pl.Utf8,
        "institution_tier_number": pl.Int64,
        "institution_tier_name": pl.Utf8,
        "state": pl.Utf8,
        "lms": pl.Utf8,
        "course": pl.Utf8,
        "version": pl.Utf8,
        "book": pl.Utf8,
        "experiment": pl.Utf8,
        "status": pl.Utf8,
        "students": pl.Int64,
        "n_opted_out": pl.Int64,
        "class_type": pl.Utf8,
        "has_supplementary_materials": pl.Utf8,
        "date_created": pl.Utf8,
        "first_response": pl.Utf8,
        "last_response": pl.Utf8,
        "completed_at": pl.Utf8,
        "instructor_id": pl.Utf8,
        "institution_id": pl.Utf8,
        "school_district_id": pl.Int64,
    },
    DataType.RESPONSES: {
        "institution_id": pl.Utf8,
        "class_id": pl.Utf8,
        "course_name": pl.Utf8,
        "release": pl.Utf8,
        "book": pl.Utf8,
        "branch": pl.Utf8,
        "student_id": pl.Utf8,
        "lms_id": pl.Utf8,
        "item_id": pl.Utf8,
        "item_type": pl.Utf8,
        "chapter": pl.Utf8,
        "page": pl.Utf8,
        "response": pl.Utf8,
        "prompt": pl.Utf8,
        "points_possible": pl.Int64,
        "points_earned": pl.Int64,
        "dt_submitted": pl.Utf8,
        "completes_page": pl.Boolean,
        "attempt": pl.Int64,
        "user_agent": pl.Utf8,
        "lrn_session_id": pl.Utf8,
        "lrn_response_id": pl.Utf8,
        "lrn_activity_reference": pl.Utf8,
        "lrn_question_reference": pl.Utf8,
        "lrn_question_position": pl.Int64,
        "lrn_type": pl.Utf8,
        "lrn_dt_started": pl.Utf8,
        "lrn_dt_saved": pl.Utf8,
        "lrn_status": pl.Utf8,
        "lrn_response_json": pl.Utf8,
        "lrn_option_0": pl.Utf8,
        "lrn_option_1": pl.Utf8,
        "lrn_option_2": pl.Utf8,
        "lrn_option_3": pl.Utf8,
        "lrn_option_4": pl.Utf8,
        "lrn_option_5": pl.Utf8,
        "lrn_option_6": pl.Utf8,
        "lrn_option_7": pl.Utf8,
        "lrn_option_8": pl.Utf8,
        "lrn_option_9": pl.Utf8,
        "lrn_option_10": pl.Utf8,
        "lrn_option_11": pl.Utf8,
        "lrn_items_api_version": pl.Utf8,
        "lrn_response_api_version": pl.Utf8,
    },
    DataType.PAGE_VIEWS: {
        "institution_id": pl.Utf8,
        "class_id": pl.Utf8,
        "student_id": pl.Utf8,
        "chapter": pl.Utf8,
        "page": pl.Utf8,
        "dt_accessed": pl.Utf8,
        "tried_again_dt": pl.Utf8,
        "tried_again_clicks": pl.Int64,
        "was_complete": pl.Boolean,
        "engaged": pl.Int64,
        "idle_brief": pl.Int64,
        "idle_long": pl.Int64,
        "off_page_brief": pl.Int64,
        "off_page_long": pl.Int64,
        "trace": pl.Utf8,
    },
    DataType.MEDIA_VIEWS: {
        "institution_id": pl.Utf8,
        "class_id": pl.Utf8,
        "student_id": pl.Utf8,
        "chapter": pl.Utf8,
        "page": pl.Utf8,
        "type": pl.Utf8,
        "media_id": pl.Utf8,
        "dt_started": pl.Utf8,
        "dt_last_event": pl.Utf8,
        "access_count": pl.Int64,
        "proportion_video": pl.Float64,
        "proportion_time": pl.Float64,
        "log_json": pl.Utf8,
    },
}


class LegacyDataType(StrEnum):
    TAGS = "tags"
    ITEMS = "items"


LEGACY_SCHEMAS: dict[LegacyDataType, SchemaDict] = {
    LegacyDataType.TAGS: {
        "tag_type": pl.Utf8,
        "tag": pl.Utf8,
        "item_id": pl.Utf8,
    },
    LegacyDataType.ITEMS: {
        "institution_id": pl.Utf8,
        "class_id": pl.Utf8,
        "item_id": pl.Utf8,
        "learnosity_id": pl.Utf8,
        "item_type": pl.Utf8,
        "chapter": pl.Utf8,
        "page": pl.Utf8,
        "dcl_pre_exercise_code": pl.Utf8,
        "dcl_sample_code": pl.Utf8,
        "dcl_solution": pl.Utf8,
        "dcl_sct": pl.Utf8,
        "dcl_hint": pl.Utf8,
        "lrn_activity_reference": pl.Utf8,
        "learnosity_question_reference": pl.Utf8,
        "learnosity_question_position": pl.Utf8,
        "learnosity_question_type": pl.Utf8,
        "learnosity_template_name": pl.Utf8,
        "learnosity_template_reference": pl.Utf8,
        "learnosity_item_status": pl.Utf8,
        "learnosity_question_data": pl.Utf8,
    },
}
