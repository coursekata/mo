import importlib.metadata
import logging
from pathlib import Path
from types import TracebackType
from typing import Annotated, cast
from uuid import UUID

import typer
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import Progress, TaskID

from mo.domain.observer import Observer, ProgressEvent
from mo.usecases.compress_usecase import CompressUseCase
from mo.usecases.organize_usecase import OrganizeUseCase

app = typer.Typer(
    name=__package__,
    help=importlib.metadata.metadata(cast(str, __package__))["Summary"],
    no_args_is_help=True,
)


def version(value: bool):
    if value:
        print(f"{__package__} v{importlib.metadata.version(cast(str, __package__))}")
        raise typer.Exit()


@app.callback()
def main(
    version: Annotated[
        bool | None,
        typer.Option("--version", "-v", callback=version, help="Show the version and exit."),
    ] = None,
):
    return


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
def compress(
    inputs: Annotated[list[Path], typer.Argument(..., help="Directories to organize.")],
    output: Annotated[
        Path,
        typer.Option("--output", "-o", help="Directory where the output data should be written."),
    ],
    move: Annotated[
        bool,
        typer.Option("--move", "-m", help="Delete the input files after compressing."),
    ] = False,
    skip_validation: Annotated[
        bool,
        typer.Option("--skip-validation", "-s", help="Skip validation of the input files."),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", "-d", help="Perform a dry run without affecting any files."),
    ] = False,
    verbose: Annotated[
        bool,
        typer.Option("--verbose", "-v", help="Enable verbose logging."),
    ] = False,
    log_file: Annotated[
        Path | None,
        typer.Option("--log-file", help="File to write logs to."),
    ] = None,
):
    config = CompressUseCase.Input(inputs=inputs, output=output)
    config.move = move
    config.skip_validation = skip_validation
    config.dry_run = dry_run

    console = setup_logging(logging.DEBUG if verbose else logging.WARNING, log_file)
    with RichProgressObserver(console) as progress_observer:
        CompressUseCase(config, [progress_observer]).execute()


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
        self.progress = Progress(console=console, transient=True)
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


if __name__ == "__main__":
    app()
