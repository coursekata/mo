from pathlib import Path
from typing import Annotated

import typer

from mo.logging import setup_logging
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
) -> None:
    config = OrganizeUseCase.Input(inputs=inputs, output=output)
    config.move = not copy
    config.ignore_duplicates = ignore or ignore_duplicates
    config.ignore_legacy = ignore or ignore_legacy
    config.dry_run = dry_run

    setup_logging(config.log_level if not verbose else "DEBUG")
    OrganizeUseCase(config).execute()


@app.command()
def process():
    pass


if __name__ == "__main__":
    app()
