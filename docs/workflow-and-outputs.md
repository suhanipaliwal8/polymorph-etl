# Workflow and outputs

How PolyMorph ETL is meant to be used, modelled on real-world migration
tools (pgloader, AWS DMS, Airbyte).

## Deployment model
PolyMorph is a CLI that runs inside the client's own environment. The client
provides connection URLs, never the database itself. Data flows directly from
the source database to the target database.

A hosted service (web UI, job queue, authentication) is out of scope for now.

## Workflow

| Step | Command | Purpose | Phase |
|---|---|---|---|
| Discover | `polymorph inspect <source-url>` | Detect the engine; list tables, columns, keys, row counts, warnings | 3 |
| Plan | `polymorph inspect --generate-manifest` | Draft a pipeline manifest to review and edit | 3 |
| Validate | `polymorph validate pipeline.yaml` | Check the manifest without touching any database | 2 |
| Dry run | `polymorph run pipeline.yaml --dry-run` | Show what would happen; write nothing | 12 |
| Migrate | `polymorph run pipeline.yaml` | Run the migration; resumable after a crash | 5-11 |
| Verify | `polymorph verify pipeline.yaml` | Row-count and SHA-256 audit of source vs target | 12 |

The source engine is detected from the connection URL. The user supplies the
target URL and the mode (`sql-to-sql` or `sql-to-nosql`).

## What the client receives
1. **The migrated target database** (the main deliverable).
2. **A migration report**: rows read, written, rejected and filtered per
   table, verification result, duration.
3. **A rejected-records file** (`dlq_errors.jsonl`): every row that could not
   be migrated, with the exact reason, so it can be fixed at the source.

Each run writes to its own folder:

```
runs/<run_id>/
  run_report.json          machine-readable summary
  report.md                human-readable summary
  dlq_errors.jsonl         rejected rows with diagnostics
  verification.json        checksum audit result
  manifest.snapshot.yaml   the exact pipeline that ran (secrets redacted)
```

## Planned report format (illustrative, will evolve)

```json
{
  "run_id": "20261123T101500Z-shop-to-mongo",
  "pipeline": "shop-to-mongo",
  "mode": "sql-to-nosql",
  "status": "completed_with_rejects",
  "duration_seconds": 42.7,
  "source": {"engine": "postgresql"},
  "target": {"engine": "mongodb"},
  "tables": [
    {"name": "customers", "rows_read": 200, "written": 197, "rejected": 3, "filtered": 0}
  ],
  "verification": {"status": "passed", "algorithm": "sha256", "mismatches": 0}
}
```

Possible `status` values: `completed`, `completed_with_rejects`, `failed`,
`interrupted` (can be resumed).

## Exit codes
| Code | Meaning |
|---|---|
| 0 | Success |
| 1 | Error (invalid manifest, failed run) |
| 2 | Command-line usage error |
| 3 | Run finished, but rows were rejected (`on_row_error: skip`); verification mismatches will use it too (Phase 12) |

## Secrets
Connection URLs are read from environment variables (`${SOURCE_URL}` in the
manifest). Passwords never appear in manifests, reports or error messages.