# Checkpointing and crash recovery

## Goal
If a run is killed (SIGKILL, power loss, network drop), running the same
command again continues from the last safely written position. Nothing is lost
and nothing is duplicated.

## The two guarantees
- **Idempotent writes** (Phase 5): writing a document twice gives the same
  result as writing it once, so repeating work is harmless.
- **Checkpoints**: after each batch the position is saved, so repeating work
  is usually unnecessary. At most one batch is repeated.

## Order matters: write first, then checkpoint
```
read batch -> translate/fold/transform -> write to MongoDB -> save checkpoint
```
Saving the checkpoint before the write could lose a batch if the process died
in between. Saving it after can at worst repeat a batch, which is harmless.

| Crash happens | Checkpoint | After re-run |
|---|---|---|
| before the write | batch N-1 | batch N is written normally |
| after the write, before the checkpoint | batch N-1 | batch N is written again; MongoDB reports it as unchanged; no duplicates |
| during the checkpoint commit | N-1 or N (SQLite commits are atomic) | one of the rows above or below |
| after the checkpoint | batch N | continues with N+1 |

## What is saved
One SQLite file per pipeline, `<state-dir>/<pipeline-name>.checkpoint.sqlite`
(`--state-dir`, default `.polymorph`). For each collection:
- status: `in_progress` or `done`
- the cursor: the primary key of the last row of the last completed batch
  (typed JSON so UUIDs, decimals and dates come back exactly)
- cumulative counters: rows read, filtered, rejected, created, updated,
  unchanged, and the counters of every embedded table
- a **fingerprint** (SHA-256) of everything that shapes the output

The cursor and the counters are written in one transaction, so they can never
disagree. The file never contains connection URLs, passwords or salts.

## Run lifecycle
- A **completed** run deletes its checkpoint. The next run starts from the
  beginning and is a full idempotent pass, exactly as before checkpoints.
- An **interrupted or failed** run keeps its checkpoint. Running the same
  command resumes: finished collections are skipped, the unfinished one
  continues after its cursor.
- `polymorph status MANIFEST` shows saved progress.
- `--fresh` deletes the checkpoint first and starts over.
- `--no-checkpoint` neither reads nor writes checkpoints.

## The fingerprint
A saved cursor is only meaningful for the configuration that produced it. The
fingerprint covers: the mode, source and target (scheme, host, database, never
credentials), the collection structure (root table, embeds, joins), the
transform rules and type overrides of the tables used, the schema of those
tables (columns, types, primary key), and the naive-timestamp time zone. A
change in any of them restarts that collection from the beginning (with a
note), which is always safe because writes are idempotent. `batch_size` and
`on_row_error` are tuning knobs and are not part of it.

## Accounting after a resume
Counters are cumulative across attempts, so
`rows read = filtered + rejected + created + updated + unchanged` still holds
and the exit code (3 when rows were rejected) reflects the whole run. The
batch that was written but not checkpointed before the crash is counted as
unchanged when it is repeated.

## One run at a time
A run holds an OS file lock (`<state-dir>/<pipeline-name>.lock`). A second run
of the same pipeline is refused. The operating system releases the lock when
the process dies, so a crash never leaves a stale lock.

## Testing crash recovery
`polymorph run ... --crash-at STAGE:N` (hidden option) ends the process
immediately, without any cleanup, like `kill -9`:
- `written:N`: after the N-th batch was written, before its checkpoint
- `checkpointed:N`: right after the N-th checkpoint

The exit code is 137. Batches are counted across the whole run.

## Limits
- Documents that a changed configuration would now filter or reject are not
  removed from MongoDB; the reconciliation report (Phase 12) will list them.
- Resume granularity is one batch.
- A finished collection is skipped on resume even if its source changed
  meanwhile; use `--fresh` to copy it again.
- Primary keys of unsupported types (for example JSON) cannot be saved; the
  collection runs without resume and a note says so.
- Checkpoints are written from a single thread; Phase 11 keeps it that way.


## Dead letters and checkpoints
The checkpoint also stores the size of the dead-letter file (`dlq_offset`,
checkpoint format v2; format v1 files from Phase 9 are migrated automatically).
The order of a batch is: documents to MongoDB, dead letters to the file with
`fsync`, then the checkpoint. On resume the file is cut back to the saved
offset, so the entries of a repeated batch are written once. See
[`dead-letter-queue.md`](dead-letter-queue.md).

Because all collections share one dead-letter file, resuming follows a chain:
finished collections are skipped, the first unfinished one resumes, and every
later collection starts from the beginning (it never ran, so nothing of it is
in the file). A collection that restarts because its configuration changed
also restarts the collections after it.

The `written:N` crash point is now "documents are in MongoDB and the dead
letters are in the file, but the checkpoint is not saved".