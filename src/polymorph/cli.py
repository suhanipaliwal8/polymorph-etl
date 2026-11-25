"""Command-line interface for PolyMorph ETL."""

import json
from pathlib import Path
from typing import Annotated, Optional

import typer

from polymorph import __version__
from polymorph.draft import render_manifest_draft
from polymorph.extractor import ExtractError, extract_batches
from polymorph.inspector import (
    InspectError,
    create_source_engine,
    inspect_database,
    render_text,
)
from polymorph.manifest import ManifestError, Mode, load_manifest, url_scheme
from polymorph.mongo_loader import LoadError
from polymorph.pipeline import (
    TABLE_HEADER,
    PipelineError,
    format_footer,
    format_table_row,
    run_pipeline,
)
app = typer.Typer(
    name="polymorph",
    help="Schema-aware ETL and migration engine.",
    no_args_is_help=True,
    add_completion=False,
)


def _fail(message: str) -> typer.Exit:
    """Print an error to stderr and return an Exit(1) for the caller to raise."""
    typer.secho(message, fg=typer.colors.RED, err=True)
    return typer.Exit(code=1)


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
        raise _fail(str(exc)) from exc

    typer.secho(f"OK: pipeline '{manifest.name}' is valid.", fg=typer.colors.GREEN)
    typer.echo(f"  mode:   {manifest.mode.value}")
    typer.echo(f"  source: {url_scheme(manifest.source.url)}")
    typer.echo(f"  target: {url_scheme(manifest.target.url)}")
    typer.echo(f"  batch:  {manifest.settings.batch_size} rows")
    tables = "all" if manifest.tables is None else str(len(manifest.tables))
    typer.echo(f"  tables: {tables}")


@app.command("inspect")
def inspect_command(
    source_url: Annotated[
        str,
        typer.Argument(
            envvar="SOURCE_URL",
            show_default=False,
            help=(
                "Source database URL. Prefer setting the SOURCE_URL environment "
                "variable: URLs typed on the command line end up in shell history."
            ),
        ),
    ],
    schema: Annotated[
        Optional[str],
        typer.Option("--schema", help="Database schema to inspect (default schema if omitted)."),
    ] = None,
    no_counts: Annotated[
        bool,
        typer.Option("--no-counts", help="Skip row counts (faster on very large tables)."),
    ] = False,
    generate_manifest: Annotated[
        Optional[Path],
        typer.Option("--generate-manifest", help="Write a draft pipeline manifest to this path."),
    ] = None,
    mode: Annotated[
        Optional[Mode],
        typer.Option("--mode", help="Pipeline mode for the draft manifest."),
    ] = None,
    force: Annotated[
        bool,
        typer.Option("--force", help="Overwrite the manifest file if it already exists."),
    ] = False,
) -> None:
    """Describe a source database: engine, tables, keys, row counts and warnings."""
    if generate_manifest is not None and mode is None:
        raise typer.BadParameter(
            "--mode is required with --generate-manifest", param_hint="--mode"
        )

    try:
        info = inspect_database(source_url, schema=schema, count_rows=not no_counts)
    except InspectError as exc:
        raise _fail(str(exc)) from exc

    typer.echo(render_text(info))

    if generate_manifest is None:
        return

    if not info.tables:
        raise _fail("Cannot generate a manifest: no tables were found.")

    draft = render_manifest_draft(info, mode)
    try:
        # "x" = create exclusively: fails if the file exists, so nothing is overwritten.
        with generate_manifest.open("w" if force else "x", encoding="utf-8") as handle:
            handle.write(draft)
    except FileExistsError as exc:
        raise _fail(f"{generate_manifest} already exists. Use --force to overwrite it.") from exc
    except OSError as exc:
        raise _fail(f"Cannot write {generate_manifest}: {exc.strerror}") from exc

    typer.secho(f"\nDraft manifest written to {generate_manifest}", fg=typer.colors.GREEN)
    typer.echo(f"Next: review it, then run: polymorph validate {generate_manifest}")



@app.command()
def preview(
    table_name: Annotated[str, typer.Argument(metavar="TABLE", help="Table to read.")],
    source_url: Annotated[
        str,
        typer.Option(
            "--source-url",
            envvar="SOURCE_URL",
            show_default=False,
            help="Source database URL (or set the SOURCE_URL environment variable).",
        ),
    ],
    limit: Annotated[
        int, typer.Option("--limit", min=1, help="Maximum number of rows to print.")
    ] = 5,
    batch_size: Annotated[
        int, typer.Option("--batch-size", min=1, help="Rows fetched per query.")
    ] = 1000,
    schema: Annotated[
        Optional[str],
        typer.Option("--schema", help="Database schema (default schema if omitted)."),
    ] = None,
) -> None:
    """Print the first rows of a table as JSON lines, read with keyset pagination.

    Rows go to stdout; per-batch progress goes to stderr, so output stays pipeable.
    """
    try:
        engine = create_source_engine(source_url)
    except InspectError as exc:
        raise _fail(str(exc)) from exc

    printed = 0
    try:
        batches = extract_batches(
            engine, table_name, batch_size=min(batch_size, limit), schema=schema
        )
        for number, batch in enumerate(batches, start=1):
            typer.echo(
                f"-- batch {number}: {len(batch.rows)} rows fetched, cursor={batch.cursor}",
                err=True,
            )
            for row in batch.rows:
                typer.echo(json.dumps(row, default=str, ensure_ascii=False))
                printed += 1
                if printed >= limit:
                    return
    except ExtractError as exc:
        raise _fail(str(exc)) from exc
    finally:
        engine.dispose()



@app.command()
def run(
    manifest_path: Annotated[
        Path, typer.Argument(help="Path to the pipeline YAML manifest.")
    ],
) -> None:
    """Run a pipeline: read the source in batches and load it into the target."""
    try:
        manifest = load_manifest(manifest_path)
    except ManifestError as exc:
        raise _fail(str(exc)) from exc

    typer.echo(
        f"Pipeline '{manifest.name}': {url_scheme(manifest.source.url)} -> "
        f"{url_scheme(manifest.target.url)} ({manifest.mode.value})"
    )

    header_printed = False

    def show_plan(notes) -> None:
        if notes:
            typer.echo("")
            typer.echo("Type mapping notes:")
            for note in notes:
                typer.echo(f"  - {note}")

    def show_table(result) -> None:
        nonlocal header_printed
        if not header_printed:
            typer.echo("")
            typer.echo(TABLE_HEADER)
            header_printed = True
        typer.echo(format_table_row(result))

    try:
        summary = run_pipeline(manifest, on_plan=show_plan, on_table_done=show_table)
    except (PipelineError, InspectError, ExtractError, LoadError) as exc:
        if header_printed:
            typer.echo(
                "\nSome tables were already written. Re-running the same command "
                "is safe: writes are idempotent.",
                err=True,
            )
        raise _fail(str(exc)) from exc

    typer.echo("")
    typer.secho(format_footer(summary), fg=typer.colors.GREEN)