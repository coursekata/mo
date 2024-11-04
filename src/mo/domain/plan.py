import logging
from abc import ABC, abstractmethod
from typing import final

from mo.domain.observer import Observable, ProgressEvent


class PlannedAction(ABC):
    @abstractmethod
    def execute(self) -> None:
        pass

    @abstractmethod
    def describe(self) -> str:
        pass


@final
class Plan(Observable[ProgressEvent]):
    def __init__(self, actions: list[PlannedAction], logger: logging.Logger | None = None) -> None:
        super().__init__()
        self.log = logger or logging.getLogger(__name__)
        self._actions: list[PlannedAction] = actions

    def add(self, action: PlannedAction) -> None:
        self._actions.append(action)

    def execute(self) -> None:
        self.log.info("Executing plan")

        event = ProgressEvent(current=0, total=len(self._actions), message="Executing plan")
        self.notify(event)

        for action in self._actions:
            self.log.debug(action.describe())
            self.notify(event.advance())
            action.execute()

    def describe(self) -> None:
        self.log.info(f"Planned actions: {self.format_plan()}")

    def format_plan(self, indent: int = 2) -> str:
        idt_str = "\n" + " " * indent
        return idt_str + idt_str.join(action.describe() for action in self._actions)
