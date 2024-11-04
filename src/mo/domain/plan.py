import logging
from abc import ABC, abstractmethod
from typing import final

from mo.domain.observer import Observable, ProgressEvent


class PlannedAction(ABC):
    @abstractmethod
    def execute(self) -> None:
        """Execute the action."""

    @abstractmethod
    def describe(self) -> str:
        """Return a human-readable description of the action for logs."""


@final
class Plan(Observable[ProgressEvent]):
    def __init__(
        self,
        actions: list[PlannedAction] | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        super().__init__()
        self.log = logger or logging.getLogger(__name__)
        self._actions: list[PlannedAction] = actions or []

    def add(self, action: PlannedAction) -> None:
        self._actions.append(action)

    def execute(self) -> None:
        self.log.info("Executing plan")

        event = ProgressEvent(current=0, total=len(self._actions), message="Executing plan")
        self.notify(event)

        for action in self._actions:
            self.log.debug(action.describe())
            action.execute()
            self.notify(event.advance())

    def describe(self) -> None:
        self.log.info(f"Planned actions: {self.format_plan()}")

    def format_plan(self, indent: int = 2) -> str:
        idt_str = "\n" + " " * indent
        return idt_str + idt_str.join(action.describe() for action in self._actions)
