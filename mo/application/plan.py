import logging

from mo.application.actions import IAction


class Plan:
    actions: list[IAction]
    """The list of actions to be executed."""

    cleanup: list[IAction]
    """The list of actions to be executed even if an error occurs."""

    def __init__(self) -> None:
        """Initialize the plan."""
        self.log = logging.getLogger(self.__class__.__name__)
        self.actions = []
        self.cleanup = []

    def __add__(self, other: "Plan") -> "Plan":
        """Combine the actions of two plans into one."""
        plan = Plan()
        plan.actions += self.actions + other.actions
        plan.cleanup += self.cleanup + other.cleanup
        return plan

    def __str__(self) -> str:
        """Return a string representation of the plan."""

        def indented(action: IAction) -> str:
            return str(action).replace("\n", "\n    ")

        string = f"Execution Plan with {len(self.actions)} actions:\n  "
        string += "\n  ".join(indented(action) for action in self.actions)
        string += f"\nCleanup Plan with {len(self.cleanup)} actions:\n  "
        string += "\n  ".join(indented(action) for action in self.cleanup)

        return string

    def execute(self) -> None:
        """Execute the plan."""
        try:
            for action in self.actions:
                self.log.debug(f"Executing: {action}")
                action.execute()
        finally:
            for action in self.cleanup:
                self.log.debug(f"Executing clean up: {action}")
                action.execute()
