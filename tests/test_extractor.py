import pytest
from sqlalchemy import text
from sqlalchemy import event

from polymorph.extractor import ExtractError, extract_batches, fetch_related, reflect_table
from polymorph.inspector import create_source_engine


@pytest.fixture()
def shop_engine(shop_url):
    engine = create_source_engine(shop_url)
    yield engine
    engine.dispose()


@pytest.fixture()
def blank_engine(tmp_path):
    path = tmp_path / "blank.db"
    path.touch()
    engine = create_source_engine(f"sqlite:///{path.as_posix()}")
    yield engine
    engine.dispose()


def execute(engine, *statements):
    with engine.begin() as conn:
        for statement in statements:
            conn.execute(text(statement))


def test_reads_every_row_once_in_key_order(shop_engine):
    ids = [
        row["id"]
        for batch in extract_batches(shop_engine, "customers", batch_size=7)
        for row in batch.rows
    ]
    assert ids == list(range(1, 51))


def test_batches_are_bounded_by_batch_size(shop_engine):
    sizes = [len(b.rows) for b in extract_batches(shop_engine, "customers", batch_size=7)]
    assert max(sizes) == 7
    assert sizes == [7, 7, 7, 7, 7, 7, 7, 1]


def test_exact_multiple_yields_no_empty_batch(shop_engine):
    sizes = [len(b.rows) for b in extract_batches(shop_engine, "customers", batch_size=10)]
    assert sizes == [10, 10, 10, 10, 10]


def test_cursor_is_the_last_primary_key_of_the_batch(shop_engine):
    first = next(iter(extract_batches(shop_engine, "customers", batch_size=7)))
    assert first.cursor == (7,)


def test_resumes_after_a_cursor(shop_engine):
    ids = [
        row["id"]
        for batch in extract_batches(shop_engine, "customers", batch_size=7, after=(20,))
        for row in batch.rows
    ]
    assert ids == list(range(21, 51))


def test_empty_table_yields_nothing(blank_engine):
    execute(blank_engine, "CREATE TABLE empty_one (id INTEGER PRIMARY KEY)")
    assert list(extract_batches(blank_engine, "empty_one")) == []


def test_composite_primary_key_pagination_and_resume(blank_engine):
    execute(
        blank_engine,
        "CREATE TABLE enrollment (student_id INTEGER, course_id INTEGER, "
        "PRIMARY KEY (student_id, course_id))",
        "INSERT INTO enrollment VALUES (2, 1), (1, 3), (3, 1), (1, 1), (2, 2), (1, 2)",
    )

    batches = list(extract_batches(blank_engine, "enrollment", batch_size=2))
    pairs = [(r["student_id"], r["course_id"]) for b in batches for r in b.rows]

    assert pairs == [(1, 1), (1, 2), (1, 3), (2, 1), (2, 2), (3, 1)]
    assert [b.cursor for b in batches] == [(1, 2), (2, 1), (3, 1)]

    resumed = extract_batches(blank_engine, "enrollment", batch_size=2, after=(1, 2))
    resumed_pairs = [(r["student_id"], r["course_id"]) for b in resumed for r in b.rows]
    assert resumed_pairs == pairs[2:]


def test_text_primary_key(blank_engine):
    execute(
        blank_engine,
        "CREATE TABLE sku (code TEXT PRIMARY KEY)",
        "INSERT INTO sku VALUES ('b'), ('a'), ('c'), ('aa')",
    )

    batches = list(extract_batches(blank_engine, "sku", batch_size=3))

    assert [r["code"] for b in batches for r in b.rows] == ["a", "aa", "b", "c"]
    assert batches[0].cursor == ("b",)


def test_table_without_primary_key_is_rejected(blank_engine):
    execute(blank_engine, "CREATE TABLE audit_log (message TEXT)")
    with pytest.raises(ExtractError, match="no primary key"):
        extract_batches(blank_engine, "audit_log")


def test_missing_table_is_reported(shop_engine):
    with pytest.raises(ExtractError, match="not found"):
        list(extract_batches(shop_engine, "nope"))


def test_errors_are_raised_before_iteration_starts(shop_engine):
    # No list() / for loop here: the error must come from the call itself.
    with pytest.raises(ExtractError):
        extract_batches(shop_engine, "nope")


def test_batch_size_must_be_positive(shop_engine):
    with pytest.raises(ValueError):
        extract_batches(shop_engine, "customers", batch_size=0)


def test_cursor_must_match_primary_key_width(shop_engine):
    with pytest.raises(ExtractError, match="Cursor"):
        extract_batches(shop_engine, "customers", after=(1, 2))


def test_rows_are_plain_dicts_with_every_column(shop_engine):
    row = next(iter(extract_batches(shop_engine, "customers", batch_size=1))).rows[0]
    assert isinstance(row, dict)
    assert set(row) == {"id", "external_id", "name", "email", "created_at", "is_active"}

def ids(rows):
    return [row["id"] for row in rows]


def test_fetch_related_returns_the_rows_of_the_given_keys_in_primary_key_order(shop_engine):
    table = reflect_table(shop_engine, "orders")
    with shop_engine.connect() as conn:
        expected = [
            row[0]
            for row in conn.execute(
                text("SELECT id FROM orders WHERE customer_id IN (1, 3) ORDER BY id")
            )
        ]

    rows = fetch_related(shop_engine, table, ["customer_id"], [(3,), (1,)])

    assert ids(rows) == expected
    assert {row["customer_id"] for row in rows} <= {1, 3}


def test_fetch_related_uses_one_query_per_chunk_of_keys(shop_engine):
    table = reflect_table(shop_engine, "orders")
    statements = []
    event.listen(
        shop_engine,
        "before_cursor_execute",
        lambda conn, cursor, statement, *args: statements.append(statement),
    )

    rows = fetch_related(
        shop_engine, table, ["customer_id"], [(1,), (2,), (3,), (4,), (5,)], chunk_size=2
    )

    assert len(statements) == 3  # 5 keys in chunks of 2
    with shop_engine.connect() as conn:
        expected = conn.execute(
            text("SELECT COUNT(*) FROM orders WHERE customer_id IN (1, 2, 3, 4, 5)")
        ).scalar_one()
    assert len(rows) == expected


def test_fetch_related_with_no_keys_does_not_query(shop_engine):
    table = reflect_table(shop_engine, "orders")
    statements = []
    event.listen(
        shop_engine,
        "before_cursor_execute",
        lambda conn, cursor, statement, *args: statements.append(statement),
    )

    assert fetch_related(shop_engine, table, ["customer_id"], []) == []
    assert statements == []


def test_fetch_related_supports_composite_keys(blank_engine):
    execute(
        blank_engine,
        "CREATE TABLE lesson (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)",
        "INSERT INTO lesson VALUES (1, 1, 1), (2, 1, 2), (3, 2, 1), (4, 2, 2)",
    )
    table = reflect_table(blank_engine, "lesson")

    rows = fetch_related(blank_engine, table, ["a", "b"], [(1, 2), (2, 1)], chunk_size=2)

    assert ids(rows) == [2, 3]


def test_fetch_related_needs_a_primary_key(blank_engine):
    execute(blank_engine, "CREATE TABLE audit_log (owner INTEGER, message TEXT)")
    table = reflect_table(blank_engine, "audit_log")
    with pytest.raises(ExtractError, match="no primary key"):
        fetch_related(blank_engine, table, ["owner"], [(1,)])


def test_fetch_related_rejects_unknown_columns_and_bad_chunk_sizes(shop_engine):
    table = reflect_table(shop_engine, "orders")
    with pytest.raises(ExtractError, match="no column"):
        fetch_related(shop_engine, table, ["nope"], [(1,)])
    with pytest.raises(ValueError):
        fetch_related(shop_engine, table, ["customer_id"], [(1,)], chunk_size=0)