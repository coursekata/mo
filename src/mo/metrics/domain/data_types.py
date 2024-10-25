from enum import StrEnum
from typing import TypeAlias

import polars as pl
from polars.datatypes import DataTypeClass

SchemaDict: TypeAlias = dict[str, pl.DataType | DataTypeClass]


class DataType(StrEnum):
    NCES = "nces"
    NCES_MAP = "nces_map"
    INSTRUCTOR_REQUESTS = "instructor_requests"


SCHEMAS: dict[DataType, SchemaDict] = {
    DataType.INSTRUCTOR_REQUESTS: {
        "first_name": pl.Utf8,
        "last_name": pl.Utf8,
        "email": pl.Utf8,
        "primary_institution": pl.Utf8,
        "institution_type": pl.Utf8,
        "approved": pl.Utf8,
        "resolved_at": pl.Utf8,
        "city": pl.Utf8,
        "state": pl.Utf8,
        "primary_role": pl.Utf8,
    },
    DataType.NCES: {
        "NCESSCH": pl.Utf8,  # School identification number
        "SURVYEAR": pl.Utf8,  # School year of reported data
        "STABR": pl.Utf8,  # State abbreviation
        "LEAID": pl.Utf8,  # School district identification number
        "LEA_NAME": pl.Utf8,  # School district name
        "SCH_NAME": pl.Utf8,  # School name
        "LSTREET1": pl.Utf8,  # Street address of school
        "LSTREET2": pl.Utf8,  # Street address of school
        "LCITY": pl.Utf8,  # City of school
        "LSTATE": pl.Utf8,  # State of school
        "LZIP": pl.Utf8,  # ZIP code of school
        "LZIP4": pl.Utf8,  # ZIP+4 code of school
        "PHONE": pl.Utf8,  # Phone number of school
        "CHARTER_TEXT": pl.Utf8,  # Charter school indicator
        "MAGNET_TEXT": pl.Utf8,  # Magnet school indicator
        "VIRTUAL": pl.Utf8,  # Virtual school indicator
        "GSLO": pl.Utf8,  # Lowest grade offered
        "GSHI": pl.Utf8,  # Highest grade offered
        "SCHOOL_LEVEL": pl.Utf8,  # School level code
        "TITLEI_TEXT": pl.Utf8,  # Title I school indicator
        "TITLEI": pl.Utf8,  # Title I school indicator
        "TITLEI_STATUS": pl.Utf8,  # Title I status
        "TITLEI_STATUS_TEXT": pl.Utf8,  # Title I status
        "STITLEI": pl.Utf8,  # Schoolwide Title I school indicator
        "STATUS": pl.Utf8,  # School operational status
        "SCHOOL_TYPE_TEXT": pl.Utf8,  # School type code
        "SY_STATUS_TEXT": pl.Utf8,  # School year-round status
        "ULOCALE": pl.Utf8,  # Urban-centric locale code
        "NMCNTY": pl.Utf8,  # County name
        "TOTFRL": pl.Int32,  # Total number of students eligible for free or reduced-price lunch
        "FRELCH": pl.Int32,  # Number of students eligible for free lunch
        "REDLCH": pl.Int32,  # Number of students eligible for reduced-price lunch
        "DIRECTCERT": pl.Int32,  # Number of students eligible for direct certification
        "PK": pl.Int32,  # Prekindergarten students
        "KG": pl.Int32,  # Kindergarten students
        "G01": pl.Int32,  # Grade 1 students
        "G02": pl.Int32,  # Grade 2 students
        "G03": pl.Int32,  # Grade 3 students
        "G04": pl.Int32,  # Grade 4 students
        "G05": pl.Int32,  # Grade 5 students
        "G06": pl.Int32,  # Grade 6 students
        "G07": pl.Int32,  # Grade 7 students
        "G08": pl.Int32,  # Grade 8 students
        "G09": pl.Int32,  # Grade 9 students
        "G10": pl.Int32,  # Grade 10 students
        "G11": pl.Int32,  # Grade 11 students
        "G12": pl.Int32,  # Grade 12 students
        "G13": pl.Int32,  # Grade 13 students
        "UG": pl.Int32,  # Ungraded Students
        "AE": pl.Int32,  # Adult Education Students
        "TOTMENROL": pl.Int32,  # Total Male Enrollment
        "TOTFENROL": pl.Int32,  # Total Female Enrollment
        "TOTAL": pl.Int32,  # Total students, all grades (includes AE)
        "MEMBER": pl.Int32,  # Total elementary/secondary students (excludes AE)
        "FTE": pl.Float64,  # Full-time equivalent (FTE) teachers
        "STUTERATIO": pl.Float64,  # Student-teacher ratio
        "AMALM": pl.Int32,  # American Indian / Alaskan Native - Male students
        "AMALF": pl.Int32,  # American Indian / Alaskan Native - Female students
        "AM": pl.Int32,  # American Indian / Alaskan Native students
        "ASALM": pl.Int32,  # Asian - Male students
        "ASALF": pl.Int32,  # Asian - Female students
        "AS_": pl.Int32,  # Asian students
        "BLALM": pl.Int32,  # Black - Male students
        "BLALF": pl.Int32,  # Black - Female students
        "BL": pl.Int32,  # Black students
        "HPALM": pl.Int32,  # Native Hawai'ian or Other Pacific Islander - Male students
        "HPALF": pl.Int32,  # Native Hawai'ian or Other Pacific Islander - Female students
        "HP": pl.Int32,  # Native Hawai'ian or Other Pacific Islander students
        "HIALM": pl.Int32,  # Hispanic - Male students
        "HIALF": pl.Int32,  # Hispanic - Female students
        "HI": pl.Int32,  # Hispanic students
        "TRALM": pl.Int32,  # Two or More Races - Male students
        "TRALF": pl.Int32,  # Two or More Races - Female students
        "TR": pl.Int32,  # STwo or More Races students
        "WHALM": pl.Int32,  # White - Male students
        "WHALF": pl.Int32,  # White - Female students
        "WH": pl.Int32,  # White students
        "LATCOD": pl.Float64,  # Latitude
        "LONCOD": pl.Float64,  # Longitude
    },
    DataType.NCES_MAP: {
        "coursekata_id": pl.Utf8,
        "coursekata_url": pl.Utf8,
        "nces_id": pl.Utf8,
    },
}
