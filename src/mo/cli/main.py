import webbrowser
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path
from typing import Annotated

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
from mo.application.processors.base import BaseProcessor
from mo.application.processors.classes import ClassesProcessor
from mo.application.processors.responses import ResponsesProcessor
from mo.application.use_cases.check import Check
from mo.application.use_cases.consolidate import Consolidate
from mo.application.use_cases.organize import (
    Organize,
    OrganizeConfig,
    OutputDirectoryNotEmptyError,
)
from mo.cli.logging import configure_logging

app = typer.Typer()


def setup_config(
    inputs: list[Path],
    output: Path,
    allow_unzip: bool,
    remove: bool,
    merge: bool,
    keep_pii: bool,
    dry_run: bool,
    log_file: Path | None,
    verbose: bool,
    exclude_columns: list[str] | None,
) -> OrganizeConfig:
    config = OrganizeConfig(inputs=inputs, output=output)
    if allow_unzip:
        config.allow_unzip = True
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
    if exclude_columns:
        config.exclude_columns += exclude_columns
    configure_logging(config.log_level.value)
    return config


def setup_organizers(config: OrganizeConfig) -> Iterable[IOrganizer]:
    return (
        ResponsesOrganizer(config),
        MediaViewsOrganizer(config),
        PageViewsOrganizer(config),
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
    allow_unzip: Annotated[
        bool,
        typer.Option(
            "--allow-unzip",
            "-u",
            help="Allow unzipping bundles before organizing them (ignores `--dry-run`).",
        ),
    ] = False,
    remove: Annotated[
        bool,
        typer.Option("--remove", "-r", help="Delete the input data after it has been processed."),
    ] = False,
    merge: Annotated[
        bool,
        typer.Option("--merge", "-m", help="Merge with any existing data in output."),
    ] = False,
    keep_pii: Annotated[
        bool,
        typer.Option(
            "--keep-pii",
            "-k",
            help="Keep personally identifiable information in class metadata.",
        ),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            "-d",
            help="Don't actually do anything, just show what would happen.",
        ),
    ] = False,
    log_file: Annotated[
        Path | None,
        typer.Option("--log-file", "-l", help="Write logs to a file as well as stdout."),
    ] = None,
    verbose: Annotated[
        bool,
        typer.Option(
            "--verbose",
            "-v",
            help="Show all debug logs in addition to errors and warnings.",
        ),
    ] = False,
    exclude_columns: Annotated[
        list[str] | None,
        typer.Option(
            "--exclude-columns",
            "-e",
            help="Columns to exclude when reading the data.",
            show_default=False,
        ),
    ] = None,
) -> None:
    """Organize your CSV download bundles."""
    config = setup_config(
        inputs=inputs,
        output=output,
        allow_unzip=allow_unzip,
        remove=remove,
        merge=merge,
        keep_pii=keep_pii,
        dry_run=dry_run,
        log_file=log_file,
        verbose=verbose,
        exclude_columns=exclude_columns,
    )
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
    allow_unzip: Annotated[
        bool,
        typer.Option(
            "--allow-unzip",
            "-u",
            help="Allow unzipping bundles before consolidating them (ignores `--dry-run`).",
        ),
    ] = False,
    remove: Annotated[
        bool,
        typer.Option("--remove", "-r", help="Delete the input data after it has been processed."),
    ] = False,
    merge: Annotated[
        bool,
        typer.Option("--merge", "-m", help="Merge with any existing data in output."),
    ] = False,
    keep_pii: Annotated[
        bool,
        typer.Option(
            "--keep-pii",
            "-k",
            help="Keep personally identifiable information in class metadata.",
        ),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            "-d",
            help="Don't actually do anything, just show what would happen.",
        ),
    ] = False,
    log_file: Annotated[
        Path | None,
        typer.Option("--log-file", "-l", help="Write logs to a file as well as stdout."),
    ] = None,
    verbose: Annotated[
        bool,
        typer.Option(
            "--verbose",
            "-v",
            help="Show all debug logs in addition to errors and warnings.",
        ),
    ] = False,
    exclude_columns: Annotated[
        list[str] | None,
        typer.Option(
            "--exclude-columns",
            "-e",
            help="Columns to exclude when reading the data.",
            show_default=False,
        ),
    ] = None,
) -> None:
    """Consolidate and compress your CSV download bundles to parquet."""
    config = setup_config(
        inputs=inputs,
        output=output,
        allow_unzip=allow_unzip,
        remove=remove,
        merge=merge,
        keep_pii=keep_pii,
        dry_run=dry_run,
        log_file=log_file,
        verbose=verbose,
        exclude_columns=exclude_columns,
    )
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
    manifest: Annotated[Path | None, typer.Option("--manifest", "-m")] = None,
    include_active: Annotated[bool, typer.Option("--include-active", "-a")] = False,
    include_test: Annotated[bool, typer.Option("--include-test")] = False,
) -> None:
    """Check that data has been downloaded for all classes in the manifest."""
    manifest = Path(data, "manifest.csv") if not manifest else Path(manifest)
    if not manifest.exists():
        raise typer.BadParameter(f"Manifest not found at {manifest}")

    print(
        "Checking data against manifest dated ",
        datetime.fromtimestamp(manifest.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
    )

    missing: list[str] = []
    processors: dict[str, BaseProcessor] = {
        "classes": ClassesProcessor(),
        "responses": ResponsesProcessor(),
    }
    for data_type, processor in processors.items():
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
