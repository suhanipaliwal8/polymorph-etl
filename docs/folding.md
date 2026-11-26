# Folding related tables into documents

## Why
A relational database spreads one real-world thing over several tables: a
customer, their profile, their orders, and each order's items. In a document
database the natural shape is one document that contains all of it, so it can
be read with a single lookup. Folding builds that document from foreign keys.

## Declaring a folded collection

```yaml
collections:
  customers:                  # name of the MongoDB collection
    from: customers           # root table: one document per row
    embed:
      - table: customer_profiles
        as: profile           # field name inside the document
        relation: one         # 0 or 1 row: an object, or null
      - table: orders
        as: orders
        relation: many        # an array (empty when there are no rows)
        embed:                # nesting: items belong to each order
          - table: order_items
            as: items
            relation: many
```

| Key | Required | Meaning |
|---|---|---|
| `from` | yes | Root table. Its primary key becomes `_id`. |
| `embed[].table` | yes | Child table to embed. |
| `embed[].as` | yes | Field name that holds the child rows. Must not be `_id` or contain `.`; must not clash with a column of the parent. |
| `embed[].relation` | yes | `one` or `many`. |
| `embed[].join` | no | `{child_column: parent_column}`. Needed only when no foreign key is declared or there is more than one. |
| `embed[].keep_foreign_key` | no (false) | Keep the child's foreign key columns inside the embedded rows. |
| `embed[].embed` | no | Nested embeds, up to 5 levels. |

`collections` and `tables` can be combined: `tables` lists tables copied flat.
With `collections` present and `tables` omitted, only the collections are
migrated. With neither, every table is copied flat.

## How the join is found
The foreign key from the child to the parent is read from the database
(reflection). If there is none, or more than one, the run stops before
writing anything and asks for a `join`. The name is `join`, not `on`: YAML
turns an unquoted `on` into `true`.

## Shape of the result
- `relation: one` gives an object, or `null` when there is no matching row
  (or the row was filtered out).
- `relation: many` gives an array that is always present, empty when there are
  no rows.
- The child's foreign key columns are removed (`keep_foreign_key: true` keeps
  them): the nesting already says who the row belongs to.
- Embedded rows keep their own primary key as an ordinary field (`id`).
- Embedded rows are ordered by their primary key, so reruns produce
  identical documents and are recognised as unchanged.
- Primary key of the root table becomes `_id`, as in a flat copy.

## Rules and folding
Rules are declared per table, as before. Each table's rules run **after its
own children were embedded**, so they can use embedded fields:

```yaml
transforms:
  orders:
    - type: compute
      into: item_count
      expression: len(items)
  customers:
    - type: compute
      into: has_profile
      expression: profile is not None
```

- An embedded table's rules see its columns minus the removed foreign key
  columns, plus the fields of its own embeds.
- `filter` on an embedded table leaves rows out of the array (or makes a `one`
  field `null`). This also settles the Phase 7 "orphan" question: rows of a
  filtered child vanish together with it.
- Primary key protection applies to the root table only; embedded rows may
  rename or mask their `id`.

## Documents are all-or-nothing
If any embedded row is rejected (a `require` fails, a value cannot be masked),
the whole document is rejected: a document silently missing some of its orders
would be data loss. Under `on_row_error: skip` the document is counted as
rejected, the reason names the failing row and the document, and the run exits
with code 3. Fix the source row and re-run: the document is created and
everything else is unchanged.
A `relation: one` with more than one matching row, or a document over
MongoDB's 16 MB limit, is rejected the same way.

## What the run shows
```
COLLECTION                     READ  FILTERED  REJECTED  CREATED  UPDATED  UNCHANGED     TIME
customers                       200         0         1      199        0          0    0.40s
  + customer_profiles           180         0         0
  + orders                      410         0         0
    + order_items             1,259         0         1
```
The first line counts **documents**. Lines starting with `+` count the source
rows consumed by that embed. `rows read` in the footer is the number of source
rows, so it can be compared with the source tables.

## How it works
For every batch of root rows (keyset pagination, as before):
1. One query per embedded table fetches the children of the whole batch:
   `WHERE fk IN (...)`, in chunks of 500 keys, ordered by primary key. This
   avoids one query per parent (the N+1 problem). Nested embeds repeat the
   step using the children just fetched.
2. Children are grouped by their key in memory.
3. Each root row is turned into a document: types are translated, foreign
   keys removed, children processed recursively, then the table's rules run.

Memory per batch is about `batch_size` times the size of an average family.
Lower `settings.batch_size` if one customer has very many orders.

Safety net: if the database matched a child row to a parent but Python's key
comparison disagrees (for example a case-insensitive collation), the run
stops instead of silently leaving the row out.

## Embed or reference?
Embed when children are few, bounded and normally read together with the
parent (a customer's orders). Keep a separate collection (a flat `tables`
entry, or a collection of its own) when children are unbounded (event logs),
are queried on their own, or could approach the 16 MB limit.

## Limits
- No snapshot between the parent and child queries: assumes a quiet source.
  Live changes belong to CDC (stretch goal).
- Child rows with a `NULL` foreign key, or pointing to a missing parent, are
  not migrated. The reconciliation report (Phase 12) will list them.
- A rejected embedded row rejects the document even if a filter would have
  dropped its parent: children are processed before the parent's rules run.
- Many-to-many relations are folded by embedding the junction table's rows,
  not the target table.
- `inspect --generate-manifest` writes flat drafts; collections are written
  by hand.