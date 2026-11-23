"""Command-line interface for PolyMorph ETL."""

from pathlib import Path
from typing import Annotated

import typer

from polymorph import __version__
from polymorph.manifest import ManifestError, load_manifest, url_scheme

app = typer.Typer(
    name="polymorph",
    help="Schema-aware ETL and migration engine.",
    no_args_is_help=True,
    add_completion=False,
)


def _show_version(value: bool) -> None:
    if value:
        typer.echo(f"polymorph {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            callback=_show_version,
            is_eager=True,
            help="Show the version and exit.",
        ),
    ] = False,
) -> None:
    """PolyMorph ETL."""


@app.command()
def validate(
    manifest_path: Annotated[
        Path, typer.Argument(help="Path to the pipeline YAML manifest.")
    ],
) -> None:
    """Check a pipeline manifest without touching any database."""
    try:
        manifest = load_manifest(manifest_path)
    except ManifestError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1) from exc

    typer.secho(
        f"OK: pipeline '{manifest.name}' is valid.", fg=typer.colors.GREEN
    )
    typer.echo(f"  mode:   {manifest.mode.value}")
    typer.echo(f"  source: {url_scheme(manifest.source.url)}")
    typer.echo(f"  target: {url_scheme(manifest.target.url)}")
    typer.echo(f"  batch:  {manifest.settings.batch_size} rows")