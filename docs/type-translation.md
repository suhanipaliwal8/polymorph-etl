# Type translation (SQL to BSON)

Each column is translated according to its **reflected SQL type**, chosen once
per table. NULL stays `null`. A value that does not fit its column type is a
translation error. The row is rejected with the table, column and primary key
(never the value itself, which may be personal data): the run stops under
`on_row_error: fail` (default) and continues, recording the row in the
dead-letter file, under `skip`.

## Mapping

| SQL type (examples) | BSON result | Rule |
|---|---|---|
| `BOOLEAN`, MySQL `TINYINT(1)` | bool | `TINYINT(1)` accepts only 0 and 1 |
| `SMALLINT`, `INTEGER`, `BIGINT` | int32 / int64 | values outside the signed 64-bit range are rejected (e.g. `BIGINT UNSIGNED`) |
| `FLOAT`, `REAL`, `DOUBLE` | double | |
| `NUMERIC`/`DECIMAL(p,s)` | **Decimal128** | exact; more than 34 digits is rejected; floats are never accepted |
| `TIMESTAMP`, `DATETIME` | date (UTC) | aware values converted to UTC; naive values read in `naive_timestamp_timezone`; truncated to milliseconds |
| `DATE` | date at midnight UTC | no time zone is applied to a plain date |
| `TIME` | string (ISO-8601) | BSON has no time-of-day type |
| `UUID` | string (lowercase, hyphenated) | also accepts 32-hex strings and 16-byte values |
| `BLOB`, `BYTEA`, `BINARY` | binary | |
| `JSON`, `JSONB`, arrays | embedded document / array | converted recursively |
| `CHAR`, `VARCHAR`, `TEXT`, `ENUM` | string | |
| anything else | by Python type | a note is printed; unsupported Python types are rejected |

## Why these choices
- **Decimal128, never float or string**: a double cannot hold
  `12345678901234.5678`; a string cannot be sorted or summed numerically.
- **UTC dates**: BSON dates are UTC instants. Naive timestamps carry no zone,
  so the tool never guesses: it uses `settings.naive_timestamp_timezone`
  (default `UTC`) and prints a note for every naive column.
- **Milliseconds**: BSON dates keep milliseconds. Sub-millisecond digits are
  truncated explicitly rather than by the driver.
- **UUID as string**: readable and identical across all three databases;
  binary would be more compact but harder to inspect.

## Hints: `type_overrides`
Some meaning cannot be seen in the type. A `CHAR(32)` that holds UUIDs is just
text to the database. Tell the tool:

```yaml
type_overrides:
  customers.external_id: uuid     # uuid | boolean | string
```

The table must be migrated and the column must exist; otherwise the run stops
before writing anything.

## Translation errors vs business rules
A text value in an integer column is a **type** problem: the row is rejected
(stage `translate`). Rejecting or fixing such
rows is the job of the transform engine (Phase 7) and the dead-letter queue
(Phase 10).

## Known limits
- `INTERVAL` and other exotic types are not supported yet.
- MySQL `TIMESTAMP` values arrive in the server session time zone.
- Ambiguous local times around daylight-saving changes use Python's default
  (`fold=0`).
- SQLite stores `NUMERIC` as floating point, so exact-decimal behaviour can
  only be verified on PostgreSQL or MySQL.