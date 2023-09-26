import logging
from pathlib import Path
from typing import Annotated, Optional

import typer

from mo.cli.logging import configure_logging
from mo.core.organize.data import DataOrganizer

# from mo.core.check.manifest import ManifestChecker

app = typer.Typer()


@app.callback()
def main() -> None:
    """MO: Methodically Organize your data."""


@app.callback()
def logs(verbose: bool = False) -> None:
    """Logging configuration."""
    configure_logging(logging.DEBUG if verbose else logging.ERROR)


@app.command()
def organize(
    input: Annotated[Path, typer.Argument()],
    output: Annotated[Optional[Path], typer.Argument()] = None,
    keep_source: Annotated[bool, typer.Option(help="Keep source files")] = False,
    yes: Annotated[bool, typer.Option("--yes", "-y", help="Skip confirmation")] = False,
) -> None:
    """Organize your data."""
    output = input if not output else output

    if not keep_source and not yes:
        warn_msg = (
            "This action consolidates your data into a handful of files and *deletes* "
            "the source files. If you need to keep the source files, use the "
            "`--keep-source` option. Proceed?"
        )
        typer.confirm(warn_msg, abort=True)

    if input == output and not yes:
        warn_msg = "Organizing in place removes all non-CourseKata content. Proceed?"
        typer.confirm(warn_msg, abort=True)
        keep_source = False

    organize = DataOrganizer()
    organize(input, output, keep_source)


# @app.command()
# def check(
#     data: Annotated[Path, typer.Argument()],
#     manifest: Annotated[Optional[Path], typer.Option("--manifest", "-m")] = None,
# ) -> None:
#     """Check that data has been downloaded for all classes in the manifest."""
#     manifest = Path(data) / "manifest.csv" if not manifest else Path(manifest)
#     if not manifest.exists():
#         raise typer.BadParameter(f"Manifest not found at {manifest}")

#     print(
#         "Checking against manifest dated ",
#         datetime.fromtimestamp(manifest.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
#     )

#     missing = list(services.check(data, manifest))
#     if not missing:
#         print("[green]All classes have been downloaded![/green]")
#         return

#     print(f"[bold red]Missing data for {len(missing)} classes:[/bold red]")
#     for id in services.check(data, manifest):
#         print(f"[link=https://coursekata.org/research/classes/view/{id}]{id}[/link]")
