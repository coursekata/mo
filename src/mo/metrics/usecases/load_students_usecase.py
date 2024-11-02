from enum import StrEnum
from itertools import chain
from pathlib import Path
from typing import cast

import polars as pl
from pydantic import BaseModel

from mo.domain.data_types import SCHEMAS, DataType
from mo.metrics.domain.institution_type import InstitutionType
from mo.metrics.usecases.extract_demographics_usecase import extract_demographics
from mo.usecases.process_responses_usecase import ProcessResponsesUseCase


class Student(BaseModel):
    class Gender(StrEnum):
        FEMALE = "Female"
        MALE = "Male"
        NON_BINARY = "Non-binary"
        OTHER = "Prefer to self-describe"

    class Race(StrEnum):
        NATIVE = "American Indian or Alaska Native"
        ASIAN = "Asian or Asian Am."
        BLACK = "Black or African Am."
        LATINO = "Hispanic, Latino, or Spanish Origin"
        MIDDLE_EASTERN = "Middle Eastern or North African"
        PACIFIC_ISLANDER = "Native Hawaiian or Pacific Islander"
        OTHER = "Other"
        WHITE = "White"

    class Education(StrEnum):
        ELEMENTARY = "Elementary or Middle School"
        SOME_HIGH_SCHOOL = "Some High School"
        HIGH_SCHOOL = "High School Graduate"
        VOCATIONAL = "Post High School Vocational Training"
        SOME_COLLEGE = "Some College"
        ASSOCIATE = "Associate's Degree"
        BACHELOR = "Bachelor's Degree"
        POST_GRADUATE = "Post Graduate Degree (Master's, Doctorate, etc.)"
        UNCERTAIN = "I do not know/uncertain"

    id: str
    class_id: str
    type: InstitutionType
    academic_year: str
    state: str | None = None
    gender: Gender | None = None
    race: Race | None = None
    maternal_education: Education | None = None


class LoadStudentsUseCase:
    def __init__(self, dir_data_raw: Path, manifest: pl.DataFrame):
        self.dir_data_raw = dir_data_raw
        self.manifest = manifest

    def execute(self) -> pl.DataFrame:
        return self.load_students(self.manifest, self.dir_data_raw)

    @staticmethod
    def load_students(manifest: pl.DataFrame, dir_data_raw: Path) -> pl.DataFrame:
        students: dict[str, dict[str, Student]] = {}
        for row in manifest.unique(["academic_year", "class_id"]).iter_rows(named=True):
            class_id = cast(str, row["class_id"])
            itype = InstitutionType(row["type"])
            academic_year = cast(str, row["academic_year"])
            state = cast(str, row["state"])
            students[academic_year] = students.get(academic_year, {})

            dir_class = dir_data_raw / class_id
            if not dir_class.exists():
                continue

            responses_path = dir_class / "responses.csv"
            if responses_path.exists():
                responses = pl.scan_csv(
                    responses_path, schema_overrides=SCHEMAS[DataType.RESPONSES]
                )
                responses = ProcessResponsesUseCase.process(responses).collect()
                for row in chain(
                    responses.iter_rows(named=True),
                    pl.scan_csv(dir_class / "media_views.csv").collect().iter_rows(named=True),
                    pl.scan_csv(dir_class / "page_views.csv").collect().iter_rows(named=True),
                ):
                    student = Student(
                        id=row["student_id"],
                        class_id=class_id,
                        type=itype,
                        academic_year=academic_year,
                        state=state,
                    )
                    students[academic_year][student.id] = student

                for row in extract_demographics(responses).collect().iter_rows(named=True):
                    student = Student(
                        id=row["student_id"],
                        class_id=class_id,
                        type=itype,
                        academic_year=academic_year,
                        state=state,
                        gender=row.get("gender"),
                        race=row.get("race"),
                        maternal_education=row.get("maternal_education"),
                    )
                    # overwrite with demographics if they exist, otherwise we keep the sparse data
                    students[academic_year][student.id] = student

        return pl.DataFrame([student for year in students.values() for student in year.values()])
