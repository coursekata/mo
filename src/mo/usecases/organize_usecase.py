import tempfile
from collections.abc import Iterable
from pathlib import Path
from typing import final

from pydantic import DirectoryPath

from mo.domain.observer import Observer, ProgressEvent
from mo.domain.plan import Plan
from mo.services.file_discovery import FileDiscoveryService
from mo.services.parsing import DataParsingService
from mo.services.validation import ValidationService
from mo.usecases.actions import ActionFactory, ActionFactoryConfig
from mo.usecases.usecase import UseCase


class Input(ActionFactoryConfig):
    inputs: list[DirectoryPath]
    dry_run: bool = False


@final
class OrganizeUseCase(UseCase):
    Input = Input

    def __init__(
        self, config: Input, observers: Iterable[Observer[ProgressEvent]] | None = None
    ) -> None:
        super().__init__()
        self.config = config
        self.observers = observers or []

    def execute(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            plan = self.prepare_plan(Path(temp_dir))
            if self.config.dry_run:
                plan.describe()
            else:
                plan.execute()

    def prepare_plan(self, extraction_directory: Path) -> Plan:
        self.log.info(f"Planning how to organize into {str(self.config.output)}")

        # discover files to process
        discovery_service = FileDiscoveryService(
            self.config.inputs,
            DataParsingService(),
            ValidationService(),
            extraction_directory,
        )
        discovery_service.register(self.observers)
        file_metadata_list = discovery_service.discover()

        # plan how to process the files
        actions = ActionFactory(self.config).make(file_metadata_list)
        plan = Plan(list(actions))
        plan.register(self.observers)

        return plan
