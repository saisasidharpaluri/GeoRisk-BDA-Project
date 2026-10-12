from __future__ import annotations

from importlib.resources import files

from georisk.contracts import SUPPORTED_CONTRACTS


def test_schemas_are_package_resources():
    for name in SUPPORTED_CONTRACTS:
        path = files("georisk") / "schemas" / "v1" / f"{name}.schema.json"
        assert path.is_file()


def test_sql_files_are_package_resources():
    directory = files("georisk") / "storage_analytics" / "sql"
    assert any(path.is_file() and path.name.endswith(".sql") for path in directory.iterdir())
