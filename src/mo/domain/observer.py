from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import Generic, Self, TypeVar
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

EventType = TypeVar("EventType")


class Observer(Generic[EventType], ABC):
    @abstractmethod
    def __call__(self, event: EventType) -> None:
        """Handle an observed event."""


class Observable(Generic[EventType]):
    def __init__(self):
        self._observers: list[Observer[EventType]] = []

    def register(self, observers: Iterable[Observer[EventType]]) -> None:
        self._observers.extend(observers)

    def notify(self, event: EventType) -> None:
        for observe in self._observers:
            observe(event)


class ProgressEvent(BaseModel):
    current: int
    total: int | None = None
    message: str = "Processing..."
    task_id: UUID = Field(default_factory=uuid4)

    def advance(self, by: int = 1, message: str | None = None) -> Self:
        self.message = message if message is not None else self.message
        self.current += by
        return self
