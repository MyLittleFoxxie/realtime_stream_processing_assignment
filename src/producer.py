"""Stream simulator: feeds the watched directory that Spark `readStream`s.

Replays `iomt_data/icu_readings.csv` in synchronized "ticks". Each tick emits
one heart-rate reading per ICU patient, stamped with a synthesized event time
(config.event_time_for_tick). Each tick becomes one newline-delimited JSON file
written *atomically* into stream_input/ (write to a dotfile, then os.replace) so
Spark never reads a half-written file.

With TICK_SECONDS=30 and a 2-minute tumbling window, four ticks fill one window.

Run (host):  python -m src.producer
"""
from __future__ import annotations

import csv
import json
import os
import time
from collections import defaultdict

from src import config


def load_readings_by_patient() -> dict[int, list[float]]:
    """patient_id -> list of heart-rate readings, in CSV order."""
    by_patient: dict[int, list[float]] = defaultdict(list)
    with open(config.READINGS_CSV, newline="") as fh:
        for row in csv.DictReader(fh):
            by_patient[int(row["patient_id"])].append(float(row["heart_rate"]))
    return by_patient


def write_tick_atomically(tick: int, records: list[dict]) -> None:
    payload = "\n".join(json.dumps(r) for r in records) + "\n"
    final = config.STREAM_INPUT_DIR / f"tick_{tick:05d}.json"
    tmp = config.STREAM_INPUT_DIR / f".tmp_tick_{tick:05d}.json"   # leading dot => ignored by Spark
    tmp.write_text(payload, encoding="utf-8")
    os.replace(tmp, final)


def main() -> None:
    if not config.READINGS_CSV.exists():
        raise FileNotFoundError(
            f"{config.READINGS_CSV} not found. Run `python -m src.prepare_data` first."
        )

    config.STREAM_INPUT_DIR.mkdir(parents=True, exist_ok=True)

    by_patient = load_readings_by_patient()
    patients = sorted(by_patient)
    max_available = min(len(v) for v in by_patient.values())
    n_ticks = min(config.MAX_TICKS, max_available)

    print(f"[producer] {len(patients)} patients, emitting {n_ticks} ticks "
          f"into {config.STREAM_INPUT_DIR}")
    print(f"[producer] event-time step {config.TICK_SECONDS}s "
          f"({config.WINDOW_SECONDS // config.TICK_SECONDS} ticks per {config.WINDOW_DURATION} window)")

    for tick in range(n_ticks):
        event_time = config.event_time_for_tick(tick).isoformat()
        records = [
            {"patient_id": p, "heart_rate": by_patient[p][tick], "event_time": event_time}
            for p in patients
        ]
        write_tick_atomically(tick, records)
        print(f"[producer] tick {tick:>3}  event_time={event_time}  ({len(records)} readings)")
        time.sleep(config.EMIT_SLEEP_SECONDS)

    # Flush sentinel: far-future, below-threshold reading per patient. Advances
    # the watermark so Spark finalizes and emits every real window.
    flush_time = config.flush_event_time(n_ticks).isoformat()
    flush_records = [
        {"patient_id": p, "heart_rate": config.SENTINEL_HR, "event_time": flush_time}
        for p in patients
    ]
    write_tick_atomically(n_ticks, flush_records)
    print(f"[producer] flush  event_time={flush_time}  ({len(flush_records)} sentinel readings)")

    print("[producer] done. Watch the Spark console for windowed averages and CLINICAL ALERTs.")


if __name__ == "__main__":
    main()
