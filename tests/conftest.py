import pytest

from polymorph.demo.seed import seed


@pytest.fixture()
def shop_url(tmp_path):
    """A freshly seeded SQLite 'shop' database. No Docker needed."""
    url = f"sqlite:///{(tmp_path / 'shop.db').as_posix()}"
    seed(url, 50)
    return url