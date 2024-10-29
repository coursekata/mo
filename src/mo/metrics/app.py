import polars as pl

from mo.logging import setup_logging
from mo.metrics.usecases.extract_demographics_usecase import ExtractDemographicsUseCase
from mo.metrics.usecases.extract_mcq import ExtractMCQUseCase
from mo.metrics.usecases.gather_activity_usecase import GatherActivityUseCase
from mo.metrics.usecases.merge_nces_usecase import MergeNCESUseCase
from mo.metrics.usecases.process_nces_usecase import ProcessNCESUseCase
from mo.metrics.usecases.tag_academic_year_usecase import TagAcademicYearUseCase


def merge_nces(config: MergeNCESUseCase.Input | None = None) -> None:
    config = MergeNCESUseCase.Input.model_validate(config or {})
    setup_logging(config.log_level)
    MergeNCESUseCase().execute(config)


def process_nces(config: ProcessNCESUseCase.Input | None = None) -> pl.LazyFrame:
    config = ProcessNCESUseCase.Input.model_validate(config or {})
    setup_logging(config.log_level)
    return ProcessNCESUseCase().execute(config).output


def gather_activity(config: GatherActivityUseCase.Input | None = None) -> pl.LazyFrame:
    config = GatherActivityUseCase.Input.model_validate(config or {})
    setup_logging(config.log_level)
    return GatherActivityUseCase().execute(config).data


def tag_academic_year(config: TagAcademicYearUseCase.Input | None = None) -> pl.LazyFrame:
    config = TagAcademicYearUseCase.Input.model_validate(config or {})
    setup_logging(config.log_level)
    return TagAcademicYearUseCase().execute(config).data


def extract_demographics(config: ExtractDemographicsUseCase.Input | None = None) -> pl.LazyFrame:
    config = ExtractDemographicsUseCase.Input.model_validate(config or {})
    setup_logging(config.log_level)
    return ExtractDemographicsUseCase().execute(config).data


def extract_mcq(config: ExtractMCQUseCase.Input | None = None) -> None:
    config = ExtractMCQUseCase.Input.model_validate(config or {})
    setup_logging(config.log_level)
    ExtractMCQUseCase().execute(config)
