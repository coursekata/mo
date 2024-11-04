from abc import ABC, abstractmethod
from typing import Generic, TypeVar

EventType = TypeVar("EventType")


class Observer(Generic[EventType], ABC):
    @abstractmethod
    def __call__(self, event: EventType) -> None:
        """Handle an observed event."""


class Observable(Generic[EventType]):
    def __init__(self):
        self._observers: list[Observer[EventType]] = []

    def register(self, observer: Observer[EventType]) -> None:
        self._observers.append(observer)

    def notify(self, event: EventType) -> None:
        for observe in self._observers:
            observe(event)
