"""Command-line interface for PolyMorph ETL."""

import json
from pathlib import Path
from typing import Annotated, Optional

import typer
from bson import json_util

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
    format_embedded_row,
    format_footer,
    format_table_row,
    make_counters,
    plan_collection,
    prefetch_children,
    process_document,
    run_pipeline,
)

PREVIEW_SCAN_LIMIT = 10_000
EXIT_ROWS_REJECTED = 3

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
    if manifest.tables is None and not manifest.collections:
        tables = "all"
    else:
        tables = str(len(manifest.tables or []))
    typer.echo(f"  tables: {tables}")
    if manifest.collections:
        typer.echo(f"  collections: {len(manifest.collections)}")
    rules = sum(len(table_rules) for table_rules in manifest.transforms.values())
    typer.echo(f"  rules:  {rules}")


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
    table_name: Annotated[
        str,
        typer.Argument(
            metavar="TABLE",
            help="Table to read (with --manifest: a table or a collection of the manifest).",
        ),
    ],
    source_url: Annotated[
        Optional[str],
        typer.Option(
            "--source-url",
            envvar="SOURCE_URL",
            show_default=False,
            help="Source database URL (or set the SOURCE_URL environment variable).",
        ),
    ] = None,
    manifest_path: Annotated[
        Optional[Path],
        typer.Option(
            "--manifest",
            help=(
                "Apply this pipeline's folding, type translation and transform rules "
                "and print the documents MongoDB would receive."
            ),
        ),
    ] = None,
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

    Rows go to stdout; progress and notes go to stderr, so output stays pipeable.
    """
    manifest = None
    if manifest_path is not None:
        try:
            manifest = load_manifest(manifest_path)
        except ManifestError as exc:
            raise _fail(str(exc)) from exc
        source_url = manifest.source.url

    if source_url is None:
        raise typer.BadParameter(
            "provide --source-url (or set SOURCE_URL), or use --manifest",
            param_hint="--source-url",
        )

    try:
        engine = create_source_engine(source_url)
    except InspectError as exc:
        raise _fail(str(exc)) from exc

    shown = scanned = rejected = filtered = 0
    try:
        plan = None
        counters = {}
        root_table = table_name
        if manifest is not None:
            plan = plan_collection(engine, table_name, manifest)
            root_table = plan.root.name
            counters = make_counters(plan)
            for note in plan.notes:
                typer.echo(f"-- note: {note}", err=True)

        if plan is not None:
            fetch_size = min(batch_size, max(limit * 5, 50))
        else:
            fetch_size = min(batch_size, limit)

        stop = False
        batches = extract_batches(engine, root_table, batch_size=fetch_size, schema=schema)
        for number, batch in enumerate(batches, start=1):
            typer.echo(
                f"-- batch {number}: {len(batch.rows)} rows fetched, cursor={batch.cursor}",
                err=True,
            )
            groups = {}
            if plan is not None:
                prefetch_children(engine, plan.embeds, batch.rows, groups)

            for row in batch.rows:
                scanned += 1
                if plan is None:
                    typer.echo(json.dumps(row, default=str, ensure_ascii=False))
                else:
                    outcome = process_document(plan, row, groups, counters)
                    if outcome.rejection is not None:
                        rejected += 1
                        typer.echo(f"-- row rejected: {outcome.rejection}", err=True)
                        continue
                    if outcome.document is None:
                        filtered += 1
                        continue
                    typer.echo(json_util.dumps(outcome.document, ensure_ascii=False))
                shown += 1
                if shown >= limit:
                    stop = True
                    break
            if stop:
                break
            if scanned >= PREVIEW_SCAN_LIMIT:
                typer.echo(f"-- stopped after scanning {scanned:,} rows", err=True)
                break

        if plan is not None:
            typer.echo(
                f"-- {shown} shown, {filtered} filtered, {rejected} rejected "
                f"({scanned} rows scanned)",
                err=True,
            )
    except (PipelineError, ExtractError, InspectError) as exc:
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
            typer.echo("Notes:")
            for note in notes:
                typer.echo(f"  - {note}")

    def show_table(result) -> None:
        nonlocal header_printed
        if not header_printed:
            typer.echo("")
            typer.echo(TABLE_HEADER)
            header_printed = True
        typer.echo(format_table_row(result))
        for embedded in result.embedded:
            typer.echo(format_embedded_row(embedded))
        if result.rejected:
            shown = len(result.rejection_samples)
            typer.echo(f"  ! {result.rejected:,} row(s) rejected; first {shown} reason(s):")
            for sample in result.rejection_samples:
                typer.echo(f"      - {sample}")

    try:
        summary = run_pipeline(manifest, on_plan=show_plan, on_table_done=show_table)
    except (PipelineError, InspectError, ExtractError, LoadError) as exc:
        if header_printed:
            typer.echo(
                "\nSome collections were already written. Re-running the same command "
                "is safe: writes are idempotent.",
                err=True,
            )
        raise _fail(str(exc)) from exc

    typer.echo("")
    color = typer.colors.YELLOW if summary.rejected else typer.colors.GREEN
    typer.secho(format_footer(summary), fg=color)

    if summary.rejected:
        typer.secho(
            f"{summary.rejected:,} document(s)/row(s) were rejected and skipped "
            "(settings.on_row_error: skip). Only the first reasons per collection are listed "
            "above; the dead-letter queue (Phase 10) will record every one.",
            fg=typer.colors.YELLOW,
            err=True,
        )
        raise typer.Exit(code=EXIT_ROWS_REJECTED)