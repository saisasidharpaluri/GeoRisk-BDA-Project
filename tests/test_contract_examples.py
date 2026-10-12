"""Executable examples of every published event contract."""

from __future__ import annotations

import json
from importlib.resources import files

import pytest

from georisk.contracts import SUPPORTED_CONTRACTS, ContractValidationError, validate_event

EXAMPLES = files("georisk") / "schemas" / "v1" / "examples"
VALID = sorted((EXAMPLES / "valid").iterdir(), key=lambda path: path.name)
INVALID = sorted((EXAMPLES / "invalid").iterdir(), key=lambda path: path.name)


@pytest.mark.parametrize("path", VALID, ids=lambda path: path.name)
def test_valid_contract_examples(path):
    contract = path.name.split("__", 1)[0]
    validate_event(contract, json.loads(path.read_text(encoding="utf-8")))


@pytest.mark.parametrize("path", INVALID, ids=lambda path: path.name)
def test_invalid_contract_examples(path):
    contract = path.name.split("__", 1)[0]
    with pytest.raises(ContractValidationError):
        validate_event(contract, json.loads(path.read_text(encoding="utf-8")))


def test_example_inventory_covers_every_contract():
    assert len(VALID) >= 6
    assert len(INVALID) >= 4
    for paths in (VALID, INVALID):
        assert {path.name.split("__", 1)[0] for path in paths} == SUPPORTED_CONTRACTS
