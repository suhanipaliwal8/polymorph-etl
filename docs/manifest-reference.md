# Manifest reference (format version 1)

| Key | Required | Description |
|---|---|---|
| `version` | yes | Format version. Currently `1`. |
| `name` | yes | Slug (lowercase letters, digits, `-`, `_`). Names checkpoint files and run folders. |
| `mode` | yes | `sql-to-sql` or `sql-to-nosql`. |
| `source.url` | yes | SQL connection URL. Use `${ENV_VAR}` for credentials. |
| `target.url` | yes | SQL URL for `sql-to-sql`, `mongodb://` or `mongodb+srv://` for `sql-to-nosql`. |
| `settings.batch_size` | no (1000) | Rows per batch, 1 to 100000. |
| `tables` | no (all) | Tables to migrate, parents before children (foreign-key order). Must be non-empty and unique. |
| `settings.naive_timestamp_timezone` | no (`UTC`) | IANA zone (e.g. `Asia/Kolkata`) used to read timestamps that carry no time zone. They are stored as UTC. |
| `type_overrides` | no | Map of `table.column` to `uuid`, `boolean` or `string`; see `type-translation.md`. |
| `settings.on_row_error` | no (`fail`) | `fail` stops at the first rejected row; `skip` counts rejected rows, loads the rest and exits with code 3. |
| `transforms` | no | Map of table name to a list of rules (`rename`, `drop`, `normalize`, `mask`, `hash`, `compute`, `filter`, `require`). See `transforms.md`. |

## Connection URLs
The URL scheme selects the database engine and the Python driver:

| Database | Example |
|---|---|
| PostgreSQL | `postgresql+psycopg://user:password@host:5432/dbname` |
| MySQL | `mysql+pymysql://user:password@host:3306/dbname` |
| SQLite | `sqlite:///path/to/file.db` |
| MongoDB | `mongodb://host:27017/dbname` |

- Install a driver with an extra: `pip install "polymorph-etl[postgres]"` or `[mysql]`.
- Special characters in passwords must be URL-encoded (`@` becomes `%40`).
- Keep URLs in environment variables (`SOURCE_URL`, `TARGET_URL`), not in files.

## Generating a draft
```bash
export SOURCE_URL="postgresql+psycopg://user:password@host:5432/dbname"
polymorph inspect                                   # describe the source
polymorph inspect --generate-manifest pipeline.yaml --mode sql-to-nosql
```
The draft lists every discovered table in foreign-key order. Review it and
remove tables you do not want to migrate. Views are not migrated.

## Example with a hint and a time zone
```yaml
settings:
  batch_size: 1000
  naive_timestamp_timezone: Asia/Kolkata

type_overrides:
  customers.external_id: uuid
```