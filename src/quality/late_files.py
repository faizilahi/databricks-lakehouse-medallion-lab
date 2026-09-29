"""Detect late-arriving bronze files relative to a silver watermark."""
from __future__ import annotations

from datetime import datetime


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00").replace("+00:00", ""))


def is_late(landing_ts: str, watermark_ts: str) -> bool:
    return parse_ts(landing_ts) > parse_ts(watermark_ts)


def late_file_report(rows: list[dict], watermark_ts: str) -> list[dict]:
    out = []
    seen = set()
    for r in rows:
        if is_late(r["_landing_ts"], watermark_ts):
            key = (r["event_date"], r["_source_file"])
            if key not in seen:
                seen.add(key)
                out.append(
                    {
                        "event_date": r["event_date"],
                        "source_file": r["_source_file"],
                        "facility_id": r["facility_id"],
                        "landing_ts": r["_landing_ts"],
                        "watermark_ts": watermark_ts,
                    }
                )
    return out
