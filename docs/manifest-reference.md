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