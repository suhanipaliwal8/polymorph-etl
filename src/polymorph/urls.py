"""Connection URL helpers shared by the manifest, inspector and pipeline."""

SQL_SCHEMES = {"postgresql", "mysql", "mariadb", "sqlite", "mssql", "oracle"}
NOSQL_SCHEMES = {"mongodb", "mongodb+srv"}


def url_scheme(url: str) -> str:
    """Return the database engine part of a URL.

    'postgresql+psycopg://...' -> 'postgresql'
    'mongodb+srv://...'        -> 'mongodb+srv'
    """
    scheme = url.split("://", 1)[0].lower()
    if scheme in NOSQL_SCHEMES:
        return scheme
    return scheme.split("+", 1)[0]