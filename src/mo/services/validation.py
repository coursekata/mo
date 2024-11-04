from abc import ABC, abstractmethod
from pathlib import Path

import polars as pl

from mo.domain.data_types import DataType, LegacyDataType
from mo.services.parsing import DataParsingService

ValidationResult = tuple[bool, str | None]


class ValidationStrategy(ABC):
    @abstractmethod
    def validate(self, file_path: Path) -> ValidationResult:
        """
        Validate the file at the given path, ensuring it is an appropriate file to process.

        Args:
            file_path (Path): The path to the file to be validated.

        Returns:
            ValidationResult: A tuple containing a boolean indicating whether the file is
            valid and a string representing the class ID of the data if it is valid and not empty.
        """


class ValidationService:
    def __init__(self):
        self.strategies: dict[DataType | LegacyDataType, ValidationStrategy] = {
            DataType.RESPONSES: InteractionDataValidationStrategy(DataType.RESPONSES),
            DataType.PAGE_VIEWS: InteractionDataValidationStrategy(DataType.PAGE_VIEWS),
            DataType.MEDIA_VIEWS: InteractionDataValidationStrategy(DataType.MEDIA_VIEWS),
            DataType.CLASSES: BasicValidationStrategy(DataType.CLASSES),
            DataType.MANIFEST: BasicValidationStrategy(DataType.MANIFEST),
            LegacyDataType.ITEMS: BasicValidationStrategy(LegacyDataType.ITEMS),
            LegacyDataType.TAGS: BasicValidationStrategy(LegacyDataType.TAGS),
        }

    def get_strategy(self, data_type: DataType | LegacyDataType) -> ValidationStrategy:
        if strategy := self.strategies.get(data_type):
            return strategy
        raise ValueError(f"No validation strategy found for file type: {data_type}")


class DummyValidationService(ValidationService):
    def get_strategy(self, data_type: DataType | LegacyDataType) -> ValidationStrategy:
        return DummyValidationStrategy()


class DummyValidationStrategy(ValidationStrategy):
    def validate(self, file_path: Path) -> ValidationResult:
        return True, None


class BasicValidationStrategy(ValidationStrategy):
    def __init__(
        self,
        data_type: DataType | LegacyDataType,
        parser: DataParsingService | None = None,
    ) -> None:
        self.parser = parser or DataParsingService()
        self.schema = self.parser.get_schema(data_type)
        self._df: pl.LazyFrame | None = None

    def validate(self, file_path: Path) -> ValidationResult:
        try:
            self._df = pl.scan_csv(file_path, schema_overrides=self.schema)
            collected_schema = self._df.collect_schema()
        except Exception:
            return False, None

        if all(column in self.schema for column in collected_schema):
            return True, None

        return False, None


class InteractionDataValidationStrategy(BasicValidationStrategy):
    def validate(self, file_path: Path) -> ValidationResult:
        try:
            valid, class_id = super().validate(file_path)
            if not valid or self._df is None:
                return valid, class_id

            # Ensure single unique class_id
            class_ids = self._df.select("class_id").drop_nulls().unique().collect()
            if len(class_ids) != 1:
                return False, None

            class_id = class_ids.get_column("class_id").cast(str)[0]
            return True, class_id
        except pl.exceptions.NoDataError:
            return True, None  # Empty file is considered valid
