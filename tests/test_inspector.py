import pytest
from sqlalchemy import create_engine, text

from polymorph.inspector import InspectError, inspect_database, render_text


def run_sql(url: str, *statements: str) -> None:
    engine = create_engine(url)
    with engine.begin() as conn:
        for statement in statements:
            conn.execute(text(statement))
    engine.dispose()


@pytest.fixture()
def blank_url(tmp_path):
    path = tmp_path / "blank.db"
    path.touch()
    return f"sqlite:///{path.as_posix()}"


def test_reports_engine_and_tables(shop_url):
    schema = inspect_database(shop_url)
    assert schema.engine == "sqlite"
    assert schema.server_version
    assert {t.name for t in schema.tables} == {
        "customers",
        "customer_profiles",
        "orders",
        "order_items",
    }


def test_tables_come_in_foreign_key_order(shop_url):
    names = [t.name for t in inspect_database(shop_url).tables]
    assert names.index("customers") < names.index("customer_profiles")
    assert names.index("customers") < names.index("orders")
    assert names.index("orders") < names.index("order_items")


def test_row_counts(shop_url):
    counts = {t.name: t.row_count for t in inspect_database(shop_url).tables}
    assert counts["customers"] == 50
    assert counts["customer_profiles"] == 45  # every 10th customer has no profile


def test_no_counts_skips_counting(shop_url):
    schema = inspect_database(shop_url, count_rows=False)
    assert all(t.row_count is None for t in schema.tables)


def test_columns_primary_keys_and_foreign_keys(shop_url):
    tables = {t.name: t for t in inspect_database(shop_url).tables}

    orders = tables["orders"]
    assert orders.primary_key == ("id",)
    (fk,) = orders.foreign_keys
    assert fk.columns == ("customer_id",)
    assert fk.referred_table == "customers"
    assert fk.referred_columns == ("id",)

    price = next(c for c in tables["order_items"].columns if c.name == "unit_price")
    assert price.type.startswith("NUMERIC")
    assert price.nullable is False


def test_warns_about_missing_and_composite_primary_keys(blank_url):
    run_sql(
        blank_url,
        "CREATE TABLE audit_log (message TEXT)",
        "CREATE TABLE enrollment (student_id INTEGER, course_id INTEGER, "
        "PRIMARY KEY (student_id, course_id))",
    )
    warnings = "\n".join(inspect_database(blank_url).warnings)
    assert "audit_log: no primary key" in warnings
    assert "enrollment: composite primary key" in warnings


def test_warns_about_foreign_key_cycles(blank_url):
    run_sql(
        blank_url,
        "CREATE TABLE a (id INTEGER PRIMARY KEY, b_id INTEGER REFERENCES b(id))",
        "CREATE TABLE b (id INTEGER PRIMARY KEY, a_id INTEGER REFERENCES a(id))",
    )
    schema = inspect_database(blank_url)
    assert any("cycle" in warning for warning in schema.warnings)
    assert {t.name for t in schema.tables} == {"a", "b"}


def test_self_reference_is_not_a_cycle(blank_url):
    run_sql(
        blank_url,
        "CREATE TABLE employee (id INTEGER PRIMARY KEY, "
        "manager_id INTEGER REFERENCES employee(id))",
    )
    schema = inspect_database(blank_url)
    assert not any("cycle" in warning for warning in schema.warnings)


def test_nosql_url_is_rejected():
    with pytest.raises(InspectError, match="SQL"):
        inspect_database("mongodb://localhost:27018/shop")


def test_malformed_url_is_rejected():
    with pytest.raises(InspectError, match="Malformed"):
        inspect_database("not a url")


def test_missing_sqlite_file_is_reported_and_not_created(tmp_path):
    path = tmp_path / "typo.db"
    with pytest.raises(InspectError, match="not found"):
        inspect_database(f"sqlite:///{path.as_posix()}")
    assert not path.exists()


def test_connection_errors_do_not_leak_the_password():
    url = "postgresql+psycopg://user:topsecret@127.0.0.1:1/db?connect_timeout=3"
    with pytest.raises(InspectError) as excinfo:
        inspect_database(url)
    assert "topsecret" not in str(excinfo.value)


def test_render_text_shows_tables_keys_and_counts(shop_url):
    output = render_text(inspect_database(shop_url))
    assert "customers" in output
    assert "(50 rows)" in output
    assert "FK (customer_id) -> customers(id)" in output