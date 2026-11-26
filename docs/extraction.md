# Extraction: keyset pagination

## Goal
Read tables of any size in bounded memory, in a way that can resume from the
last committed position after a crash.

## Why not OFFSET?
`SELECT ... ORDER BY id LIMIT 1000 OFFSET 5000000` makes the database walk
and discard five million rows before returning one thousand. Every page is
slower than the previous one, so total cost grows quadratically. It is also
unstable: rows inserted or deleted while the migration runs shift every later
page, so rows can be skipped or read twice.

## Keyset (seek) pagination
```sql
SELECT * FROM orders WHERE id > :last_seen ORDER BY id LIMIT 1000
```
The primary-key index jumps straight to the right place, so every page costs
the same. `last_seen` is the **cursor**. It is also exactly what a checkpoint
needs to store (Phase 9).

### Composite keys
For a key `(a, b)` and cursor `(x, y)` the condition is
`a > x OR (a = x AND b > y)`. This is the portable form of `(a, b) > (x, y)`,
which not every database supports.

## Design
- One short query, and one short transaction, per batch. A long-lived
  server-side cursor would pin a transaction for the whole migration and
  could not be resumed after a crash.
- Batches come from a generator, so only one batch is in memory at a time.
- Rows are plain dicts keyed by column name; type translation happens later.
- `after=<cursor>` resumes strictly after a previously seen key.

## Limitations
- Tables need a primary key; tables without one are rejected with a clear
  message.
- No snapshot: rows inserted behind the cursor while a table is being read are
  not seen. Live changes are the job of CDC (stretch goal).
- Ordering and comparison happen in the database, so text keys follow the
  database's collation consistently.


## Fetching related rows (folding)
Folded collections need the children of every row in a batch. Instead of one
query per parent row (the N+1 problem), `fetch_related` runs one query per
chunk of 500 parent keys (`WHERE fk IN (...)`, or an `OR` of `AND`s for
composite keys), ordered by primary key. See [`folding.md`](folding.md).