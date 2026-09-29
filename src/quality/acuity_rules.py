"""Acuity code validation shared by silver and quarantine writers."""
from __future__ import annotations

VALID_ACUITY = frozenset({"A1", "A2", "A3", "A4", "B1", "B2", "C1"})


def is_valid_acuity(code: str | None) -> bool:
    if code is None:
        return False
    return str(code).strip().upper() in VALID_ACUITY


def classify_row(acuity_code: str | None) -> str:
    return "ACCEPT" if is_valid_acuity(acuity_code) else "QUARANTINE"
