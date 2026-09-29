"""Partition completeness before silver advances watermark."""
from __future__ import annotations


def expected_facilities(manifest_rows: list[dict], event_date: str) -> set[str]:
    return {r["facility_id"] for r in manifest_rows if r["event_date"] == event_date}


def arrived_facilities(bronze_rows: list[dict], event_date: str) -> set[str]:
    return {r["facility_id"] for r in bronze_rows if r["event_date"] == event_date}


def is_complete(expected: set[str], arrived: set[str]) -> tuple[bool, set[str]]:
    missing = expected - arrived
    return len(missing) == 0, missing


def watermark_should_advance(
    manifest_rows: list[dict],
    bronze_rows: list[dict],
    event_date: str,
    min_facility_coverage: float = 1.0,
) -> dict:
    exp = expected_facilities(manifest_rows, event_date)
    if not exp:
        exp = arrived_facilities(bronze_rows, event_date)
    arr = arrived_facilities(bronze_rows, event_date)
    coverage = (len(arr) / len(exp)) if exp else 0.0
    ok, missing = is_complete(exp, arr)
    return {
        "event_date": event_date,
        "expected": len(exp),
        "arrived": len(arr),
        "coverage": round(coverage, 4),
        "complete": coverage >= min_facility_coverage and ok,
        "missing_facilities": sorted(missing),
    }
