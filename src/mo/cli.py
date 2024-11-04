import logging
from pathlib import Path
from types import TracebackType
from typing import Annotated
from uuid import UUID

import typer
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import Progress, TaskID

from mo.domain.observer import Observer, ProgressEvent
from mo.usecases.organize_usecase import OrganizeUseCase

app = typer.Typer()


@app.command()
def organize(
    inputs: Annotated[list[Path], typer.Argument(..., help="Directories to organize.")],
    output: Annotated[
        Path,
        typer.Option("--output", "-o", help="Directory where the output data should be written."),
    ],
    copy: Annotated[
        bool,
        typer.Option("--copy", "-c", help="Copy the files instead of moving them."),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", "-d", help="Perform a dry run without affecting any files."),
    ] = False,
    verbose: Annotated[
        bool,
        typer.Option("--verbose", "-v", help="Enable verbose logging."),
    ] = False,
    ignore: Annotated[
        bool,
        typer.Option("--ignore", "-i", help="Don't delete duplicate input files and legacy types."),
    ] = False,
    ignore_legacy: Annotated[
        bool,
        typer.Option("--ignore-legacy", help="Don't delete legacy data types."),
    ] = False,
    ignore_duplicates: Annotated[
        bool,
        typer.Option("--ignore-duplicates", help="Don't delete duplicate input files."),
    ] = False,
    log_file: Annotated[
        Path | None,
        typer.Option("--log-file", help="File to write logs to."),
    ] = None,
) -> None:
    config = OrganizeUseCase.Input(inputs=inputs, output=output)
    config.move = not copy
    config.ignore_duplicates = ignore or ignore_duplicates
    config.ignore_legacy = ignore or ignore_legacy
    config.dry_run = dry_run

    console = setup_logging(logging.DEBUG if verbose else logging.WARNING, log_file)
    with RichProgressObserver(console) as progress_observer:
        OrganizeUseCase(config, [progress_observer]).execute()


@app.command()
def process():
    pass


def setup_logging(level: int | str = logging.DEBUG, log_file: Path | None = None) -> Console:
    console = Console(soft_wrap=True)

    handlers: list[logging.Handler] = [RichHandler(console=console, show_level=False)]
    if log_file:
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=handlers,
    )

    return console


class RichProgressObserver(Observer[ProgressEvent]):
    def __init__(self, console: Console) -> None:
        self.progress = Progress(console=console)
        self.tasks: dict[UUID, TaskID] = {}

    def __enter__(self):
        """Start the Rich progress display."""
        self.progress.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ):
        """Stop the Rich progress display."""
        self.progress.__exit__(exc_type, exc_val, exc_tb)

    def __call__(self, event: ProgressEvent) -> None:
        """Handle a ProgressEvent by updating the appropriate task."""
        if event.task_id not in self.tasks:
            self.tasks[event.task_id] = self.progress.add_task(
                event.message, total=event.total, completed=event.current
            )

        self.progress.update(self.tasks[event.task_id], completed=event.current)
        if event.total and event.total == event.current:
            self.progress.remove_task(self.tasks[event.task_id])


if __name__ == "__main__":
    app()
