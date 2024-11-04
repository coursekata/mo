import logging
from abc import ABC, abstractmethod
from typing import final


class PlannedAction(ABC):
    @abstractmethod
    def execute(self) -> None:
        pass

    @abstractmethod
    def describe(self) -> str:
        pass


@final
class Plan:
    def __init__(self, actions: list[PlannedAction], logger: logging.Logger | None = None) -> None:
        self.log = logger or logging.getLogger(__name__)
        self._actions: list[PlannedAction] = actions

    def add(self, action: PlannedAction) -> None:
        self._actions.append(action)

    def execute(self) -> None:
        self.log.info("Executing plan")
        for action in self._actions:
            self.log.debug(action.describe())
            action.execute()

    def describe(self) -> None:
        self.log.info(f"Planned actions: {self.format_plan()}")

    def format_plan(self, indent: int = 2) -> str:
        idt_str = "\n" + " " * indent
        return idt_str + idt_str.join(action.describe() for action in self._actions)
