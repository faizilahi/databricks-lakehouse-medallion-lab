"""Regenerate synthetic EHR incident drops (idempotent)."""
from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
RAW.mkdir(parents=True, exist_ok=True)
random.seed(42)


def main() -> None:
    facilities = [f"F-{i:03d}" for i in range(1, 31)]
    acuity_ok = ["A1", "A2", "A3", "A4", "B1", "B2"]
    acuity_bad = ["XX", "??", "Z9"]
    rows = []
    base = datetime(2025, 8, 14, 0, 0, 0)
    for day in range(5):
        event_date = (base + timedelta(days=day)).strftime("%Y-%m-%d")
        for fac in facilities:
            n = random.randint(8, 22)
            for k in range(n):
                late = day == 2 and fac in ("F-017", "F-022") and k < 6
                bad = random.random() < 0.03
                landing = base + timedelta(days=day, hours=2, minutes=k)
                if late:
                    landing = base + timedelta(days=4, hours=1, minutes=k)
                rows.append(
                    {
                        "incident_id": f"INC-{event_date.replace('-', '')}-{fac}-{k:03d}",
                        "facility_id": fac,
                        "event_date": event_date,
                        "event_ts": (base + timedelta(days=day, hours=8, minutes=k * 3)).isoformat(),
                        "acuity_code": random.choice(acuity_bad) if bad else random.choice(acuity_ok),
                        "severity_band": random.choice(["LOW", "MED", "HIGH"]),
                        "length_of_stay_hrs": round(random.uniform(0.5, 48), 1),
                        "_source_file": f"ehr_{event_date}_{fac}.csv",
                        "_landing_ts": landing.isoformat() + "Z",
                    }
                )
    dim = [
        {
            "facility_id": f,
            "region": random.choice(["NYC", "LI", "NJ", "WESTCHESTER"]),
            "beds": random.randint(40, 400),
        }
        for f in facilities
    ]
    with (RAW / "incident_events.csv").open("w", newline="", encoding="utf-8") as fh:
        wtr = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        wtr.writeheader()
        wtr.writerows(rows)
    with (RAW / "dim_facility.csv").open("w", newline="", encoding="utf-8") as fh:
        wtr = csv.DictWriter(fh, fieldnames=list(dim[0].keys()))
        wtr.writeheader()
        wtr.writerows(dim)
    print(f"incidents={len(rows)} facilities={len(dim)}")


if __name__ == "__main__":
    main()
