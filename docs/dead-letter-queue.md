# Dead-letter queue

## Why
Production data contains **poison records**: rows that are valid for the source
database but cannot be migrated (an invalid email, a negative quantity, a
number that does not fit the target type). One such row must not stop a
10-million-row migration, and it must not vanish silently either. With
`settings.on_row_error: skip` every rejected document is written to a
dead-letter file with the exact reason, and the run continues.

## What gets rejected
| Stage | Meaning |
|---|---|
| `translate` | a value does not fit its column type (text in an integer column, more than 34 decimal digits, an integer beyond 64 bits) |
| `transform` | a rule refused the row (`require` failed, an email could not be masked, an expression failed) |
| `relation` | a `relation: one` embed found more than one row |
| `document` | the finished document exceeds MongoDB's 16 MB limit |

Under `on_row_error: fail` (the default) the run stops at the first rejected
row; that one row is still recorded in the file, so the reason is not lost.

## The file
Default location: `<state-dir>/<pipeline>.dlq_errors.jsonl`
(`.polymorph/shop-folded.dlq_errors.jsonl`). Change it with `--dlq-file`.
One JSON object per line:

```json
{"timestamp": "2026-11-26T10:15:00Z", "pipeline": "shop-folded", "collection": "customers",
 "document": {"table": "customers", "key": {"id": 17}},
 "cause": {"stage": "transform", "table": "order_items", "key": {"id": 5},
           "reason": "quantity must be positive"},
 "message": "order_items: quantity must be positive (row id=5); document rejected: customers id=17"}
```

| Field | Meaning |
|---|---|
| `document` | the rejected document: root table and primary key |
| `cause` | the row that made it fail (it can be an embedded row): stage, table, key, reason |
| `message` | the same text that the run output shows |
| `row` | the raw source row of the cause, **only** with `settings.dlq_include_rows: true` |

Primary key values are enough to find a row: `SELECT * FROM order_items WHERE id = 5`.

## Privacy
Entries contain table names, keys and reasons, never row data, so the file is
safe to share. `dlq_include_rows: true` adds the **raw source row**, which can
contain personal data that the pipeline would have masked. Protect such a file
like the source database. The file is plain ASCII (non-ASCII text is escaped),
so even hostile data cannot break writing it.

## Lifecycle
- The file always describes the **latest run**: a new run starts it empty, so
  fixing the source and re-running (writes are idempotent) leaves an empty
  file, with no stale entries.
- An interrupted run that is resumed continues the same file (see below).

## Crash safety
```
write documents to MongoDB -> append dead letters + fsync -> save checkpoint
```
The checkpoint stores the **size of the dead-letter file** (`dlq_offset`). On
resume the file is truncated to that size, which removes the entries of the
batch that was not checkpointed; the batch is processed again and writes them
once. So there are never duplicate or missing entries, even after `kill -9`.
If the file is shorter than the checkpoint expects (deleted or replaced),
the run refuses to continue and asks for `--fresh`.

All collections share one file, so their offsets form a chain: once a
collection starts over (configuration changed, no saved progress), the
collections after it start over too. That is always safe because writes are
idempotent.

## Commands
```bash
polymorph dlq pipeline.yaml              # summary by collection, stage, table and reason
polymorph dlq pipeline.yaml --limit 20   # show the first 20 entries
polymorph dlq pipeline.yaml --file other.jsonl
polymorph status pipeline.yaml           # also shows the dead-letter file
```

## Replaying
There is no replay command. Fix the rows in the source (or adjust the rules)
and run the pipeline again: documents that were rejected are created, all
others are reported as unchanged.

## Limits
- With `dlq_include_rows` a row with huge binary columns makes huge entries.
- Resuming from a checkpoint saved before this feature existed cannot recover
  that earlier run's rejections; the run says so (use `--fresh` for a complete file).
- The file is rewritten per run. Phase 12 moves it into a per-run folder.