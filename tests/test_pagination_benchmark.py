from polymorph.demo.pagination_benchmark import run


def test_both_strategies_read_every_row():
    result = run(rows=2_000, batch_size=300)
    assert result.offset_rows == 2_000
    assert result.keyset_rows == 2_000