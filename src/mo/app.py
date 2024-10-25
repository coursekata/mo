from mo.logging import setup_logging
from mo.usecases.consolidate_usecase import ConsolidateUseCase
from mo.usecases.process_responses_usecase import ProcessResponsesUseCase


def consolidate(config: ConsolidateUseCase.Input | None = None) -> None:
    config = ConsolidateUseCase.Input.model_validate(config or {})
    setup_logging(config.log_level)
    ConsolidateUseCase().execute(config)


def process_responses(config: ProcessResponsesUseCase.Input | None = None) -> None:
    config = ProcessResponsesUseCase.Input.model_validate(config or {})
    setup_logging(config.log_level)
    ProcessResponsesUseCase().execute(config)
