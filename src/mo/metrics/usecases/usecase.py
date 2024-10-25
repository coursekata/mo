import logging
from abc import ABC, abstractmethod
from typing import Any


class UseCase(ABC):
    def __init__(self):
        self.log = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def execute(self, input: Any) -> Any:
        """Executes the use case."""
