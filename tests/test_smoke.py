import polymorph


def test_package_exposes_version():
    assert polymorph.__version__ == "0.1.0"