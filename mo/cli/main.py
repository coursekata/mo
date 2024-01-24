import webbrowser
from datetime import datetime
from pathlib import Path
from typing import Annotated, Iterable, Optional

import typer
from rich import print

from mo.application.config import LogLevel
from mo.application.interfaces.organizer import IOrganizer
from mo.application.organizers import (
    ClassesOrganizer,
    MediaViewsOrganizer,
    PageViewsOrganizer,
    ResponsesOrganizer,
    SupplementaryOrganizer,
    TagsOrganizer,
)
from mo.application.organizers.items import ItemsOrganizer
from mo.application.processors.classes import ClassesProcessor
from mo.application.processors.responses import ResponsesProcessor
from mo.application.use_cases.check import Check
from mo.application.use_cases.consolidate import Consolidate
from mo.application.use_cases.organize import Organize, OrganizeConfig, OutputDirectoryNotEmptyError
from mo.cli.logging import configure_logging

app = typer.Typer()


def setup_config(
    inputs: list[Path],
    output: Path,
    remove: bool,
    merge: bool,
    keep_pii: bool,
    dry_run: bool,
    log_file: Path | None,
    verbose: bool,
) -> OrganizeConfig:
    config = OrganizeConfig(inputs=inputs, output=output)
    if remove:
        config.remove = True
    if merge:
        config.merge = True
    if keep_pii:
        config.strip_pii = False
    if dry_run:
        config.dry_run = True
    if log_file:
        config.log_file = log_file
    if verbose:
        config.log_level = LogLevel.debug
    configure_logging(config.log_level)
    return config


def setup_organizers(config: OrganizeConfig) -> Iterable[IOrganizer]:
    return (
        MediaViewsOrganizer(config),
        PageViewsOrganizer(config),
        ResponsesOrganizer(config),
        SupplementaryOrganizer(config),
        TagsOrganizer(config),
        ItemsOrganizer(config),
        ClassesOrganizer(config),
        # TODO: Jupyter
    )


@app.callback()
def main() -> None:
    """MO: Methodically Organize your data."""


@app.command()
def organize(
    inputs: Annotated[list[Path], typer.Argument()],
    output: Annotated[
        Path,
        typer.Option("--output", "-o", help="Directory where the output data should be written."),
    ],
    remove: Annotated[
        bool,
        typer.Option("--remove", "-r", help="Delete the input data after it has been processed."),
    ] = False,
    merge: Annotated[
        bool, typer.Option("--merge", "-m", help="Merge with any existing data in output.")
    ] = False,
    keep_pii: Annotated[
        bool,
        typer.Option(
            "--keep-pii", "-k", help="Keep personally identifiable information in class metadata."
        ),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run", "-d", help="Don't actually do anything, just show what would happen."
        ),
    ] = False,
    log_file: Annotated[
        Optional[Path],
        typer.Option("--log-file", "-l", help="Write logs to a file as well as stdout."),
    ] = None,
    verbose: Annotated[
        bool,
        typer.Option(
            "--verbose", "-v", help="Show all debug logs in addition to errors and warnings."
        ),
    ] = False,
) -> None:
    """Organize your CSV download bundles."""
    config = setup_config(inputs, output, remove, merge, keep_pii, dry_run, log_file, verbose)
    organizers = setup_organizers(config)
    try:
        Organize(config, organizers).execute()
    except OutputDirectoryNotEmptyError:
        if typer.confirm(
            f"Output directory {config.output} is not empty, so this operation would overwrite any "
            "existing files that have the same names. Do you want to continue?"
        ):
            config.merge = True
            Organize(config, organizers).execute()


@app.command()
def consolidate(
    inputs: Annotated[list[Path], typer.Argument()],
    output: Annotated[
        Path,
        typer.Option("--output", "-o", help="Directory where the output data should be written."),
    ],
    remove: Annotated[
        bool,
        typer.Option("--remove", "-r", help="Delete the input data after it has been processed."),
    ] = False,
    merge: Annotated[
        bool, typer.Option("--merge", "-m", help="Merge with any existing data in output.")
    ] = False,
    keep_pii: Annotated[
        bool,
        typer.Option(
            "--keep-pii", "-k", help="Keep personally identifiable information in class metadata."
        ),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run", "-d", help="Don't actually do anything, just show what would happen."
        ),
    ] = False,
    log_file: Annotated[
        Optional[Path],
        typer.Option("--log-file", "-l", help="Write logs to a file as well as stdout."),
    ] = None,
    verbose: Annotated[
        bool,
        typer.Option(
            "--verbose", "-v", help="Show all debug logs in addition to errors and warnings."
        ),
    ] = False,
) -> None:
    """Consolidate and compress your CSV download bundles to parquet."""
    config = setup_config(inputs, output, remove, merge, keep_pii, dry_run, log_file, verbose)
    organizers = setup_organizers(config)
    try:
        Consolidate(config, organizers).execute()
    except OutputDirectoryNotEmptyError:
        if typer.confirm(
            f"Output directory {config.output} is not empty, so this operation would overwrite any "
            "existing files that have the same names. Do you want to continue?"
        ):
            config.merge = True
            Consolidate(config, organizers).execute()


@app.command()
def check(
    data: Annotated[Path, typer.Argument()],
    manifest: Annotated[Optional[Path], typer.Option("--manifest", "-m")] = None,
) -> None:
    """Check that data has been downloaded for all classes in the manifest."""
    manifest = Path(data, "manifest.csv") if not manifest else Path(manifest)
    if not manifest.exists():
        raise typer.BadParameter(f"Manifest not found at {manifest}")

    print(
        "Checking data against manifest dated ",
        datetime.fromtimestamp(manifest.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
    )

    missing = []
    for data_type, processor in {
        "classes": ClassesProcessor(),
        "responses": ResponsesProcessor(),
    }.items():
        data_file = Path(data, f"{data_type}.parquet")
        if not data_file.exists():
            raise typer.BadParameter(f"{data_type.capitalize()} file not found at {data_file}")

        checker = Check(manifest, data_file, processor)
        missing_data_type = checker.execute()
        missing += missing_data_type
        if not missing_data_type:
            print(f"[green]All {data_type} have been downloaded![/green]")

    if missing:
        print("[red]The following classes have not been downloaded or are missing data:[/red]")
        link_prefix = "https://coursekata.org/research/classes/view"
        for class_id in missing:
            print(f"  {link_prefix}/{class_id}")

        if typer.confirm("Do you want to open these links in your browser?", abort=True):
            for class_id in missing:
                webbrowser.open(f"{link_prefix}/{class_id}")
