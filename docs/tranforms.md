# Transforms

Rules run on every row between type translation and loading. They are
declared per table in the manifest and applied in the order listed.

```
extract -> translate types -> transform rules -> build document -> load
```

Rules see translated values: text, numbers, booleans, UTC datetimes and
`Decimal128` (shown to expressions as an exact `Decimal`).

## Example

```yaml
settings:
  on_row_error: skip          # fail (default) | skip

transforms:
  customers:
    - type: normalize
      fields: [name, email]
      ops: [nfc, collapse_whitespace, nullify_empty]
    - type: require
      expression: email is not None
      message: customer email is required
    - type: hash
      field: email
      into: email_hash
      salt: ${HASH_SALT}
    - type: mask
      field: email
      style: email
```

## Rules

| Rule | Keys | What it does |
|---|---|---|
| `rename` | `mapping: {old: new}` | Renames fields (swaps are allowed). Field order is kept. |
| `drop` | `fields: [...]` | Removes fields. |
| `normalize` | `fields`, `ops` | Text clean-up, applied in order: `trim`, `collapse_whitespace`, `lower`, `upper`, `nfc` (Unicode normalisation), `nullify_empty` (empty text becomes null). |
| `mask` | `field`, `style`, `keep_last` | `email`: `asha@example.com` becomes `a***@***.com`. `partial`: keeps the last `keep_last` characters (default 4, never more than half the value). `redact`: replaces any value with `[REDACTED]`. |
| `hash` | `field`, `into`, `salt` | SHA-256 hex digest; with `salt` it is HMAC-SHA256. `into` writes the digest to a new field and keeps the original; without it the field is replaced. Accepts text and integers. |
| `compute` | `into`, `expression` | Adds a new field computed from other fields. |
| `filter` | `expression` | Keeps rows where the expression is true. False or null drops the row (counted as **filtered**). |
| `require` | `expression`, `message` | Rows where the expression is not true are **rejected** with the message. |

Null values pass through `normalize`, `mask` and `hash` unchanged.

## Filtered, rejected, and `on_row_error`
- **Filtered**: a `filter` rule dropped the row on purpose. Never an error.
- **Rejected**: the row is invalid (`require` failed, an email could not be
  masked, an expression failed on that row).
  - `on_row_error: fail` (default): the run stops at the first rejected row.
    Earlier batches stay loaded; re-running is safe (idempotent writes).
  - `on_row_error: skip`: rejected rows are counted and the first ten reasons
    per table are printed. The run ends with **exit code 3**. The dead-letter
    queue (Phase 10) will record every rejected row.

Type errors (for example text in an integer column) always stop the run.

Every row is accounted for:
`rows read = filtered + rejected + created + updated + unchanged`.

## Primary key columns
Primary key columns cannot be renamed, dropped, masked, hashed in place or
normalised: `_id` and resume cursors depend on them. A hash can still be
written to a new field with `into`. A non-key column named `_id` must be
renamed (it would collide with MongoDB's `_id`).

## Order matters
Rules are checked in order, so later rules see the fields earlier rules
produced. After `rename: {qty: quantity}` an expression must say `quantity`.
Hash *before* you mask if you want a hash of the original value, and
normalise (trim, lower) *before* hashing so that equal values hash equally.

## Hashing guidance
An unsalted hash of a guessable value (email, phone) can be reversed by
trying candidates. Use a secret `salt` (`salt: ${HASH_SALT}`, at least 8
characters). Unsalted hashes are allowed but announced in the run output.
Use the same salt on every run: the output must be deterministic, or
re-running would update every document.

## Expressions
Python-like syntax, parsed with `ast` and run by a small interpreter.
`eval()` is never used.

- Field names are written as they are (`unit_price`); a name that is not a
  valid identifier (`order-id`) cannot be referenced.
- Constants: `'text'`, numbers, `True`, `False`, `None`.
- Operators: `+ - * / // %`, comparisons (also chained: `1 < x <= 3`), `in`,
  `not in`, `is None`, `is not None`, `and`, `or`, `not`, and
  `a if cond else b`. Lists `[...]` for `in`.
- Functions: `lower`, `upper`, `trim`, `len`, `abs`, `round`, `coalesce`,
  `str`, `int`, `float`, `decimal`, `datetime`, `contains`, `startswith`,
  `endswith`.
- Not allowed: attribute access, indexing, lambdas, comprehensions, f-strings,
  `**`, repeating text (`'a' * 5`) and `%` formatting.

Null handling: comparing null with `<`, `>` or arithmetic raises an error and
rejects that row. Test for it (`x is not None and x > 1`) or use
`coalesce(x, 0)`.

Money: exact decimals cannot be mixed with floats. Write
`total_amount * decimal('1.18')`, not `total_amount * 1.18`.

Dates: `created_at >= datetime('2024-06-01')` (naive text is read as UTC).

If an expression fails on a row, the row is rejected and the message names
the exception type only, never the data.

## Adding a new rule type
A rule is a small Pydantic model with `type: Literal["name"]`, a `check()`
(static: which fields exist afterwards) and an `apply()` (runtime). Add the
class to the `Rule` union in `polymorph/transform.py`; the manifest, the
pre-flight checks and the pipeline pick it up automatically.

## Limits
- Rules work on one row at a time: no cross-row or cross-table logic until
  join folding (Phase 8).
- Filtering a table does not filter tables that refer to it, so a flat copy
  can contain orphaned rows.