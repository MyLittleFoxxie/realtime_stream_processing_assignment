"""Shared constants and the canonical event-time scheme.

Importable everywhere (pure stdlib, no pandas / no pyspark) so the producer and
the Spark job agree on the *exact* same windowing parameters and synthesized
timestamps.
"""
from __future__ import annotations

import datetime as _dt
from pathlib import Path

# --- Stream shaping -------------------------------------------------------
POOL_SIZE = 30                     # number of simulated ICU beds (patients)
TICK_SECONDS = 30                  # event-time advance between successive readings of a patient
MAX_TICKS = 60                     # cap readings/patient so the demo finishes quickly (60*30s = 30 min event-time)
EMIT_SLEEP_SECONDS = 1.0           # real-time pause between ticks (so the console demo is watchable)

# A final far-future "flush" reading per patient advances the watermark past all
# real windows so append-mode aggregation finalizes (and emits) every one of them.
# It is below threshold, so it never produces an alert.
FLUSH_GAP_SECONDS = 600            # 10 min after the last real reading
SENTINEL_HR = 70.0                 # benign heart rate for the flush tick

# Fixed base so the producer generates deterministic event timestamps.
BASE_TIME = _dt.datetime(2026, 6, 14, 0, 0, 0, tzinfo=_dt.timezone.utc)

# --- Windowing / alerting -------------------------------------------------
WINDOW_DURATION = "2 minutes"      # tumbling window (Scenario B)
WATERMARK_DELAY = "4 minutes"      # bounds aggregation state; tolerates minor out-of-order
WINDOW_SECONDS = 120
HR_THRESHOLD = 100.0               # bpm; "exceeding 100 bpm" => strictly greater
CONSECUTIVE_REQUIRED = 2           # consecutive breaching windows needed to fire a clinical alert

# --- Paths (resolved relative to assignment_6/, works on host and in /app) -
ASSIGNMENT_DIR = Path(__file__).resolve().parent.parent
READINGS_CSV = ASSIGNMENT_DIR / "iomt_data" / "icu_readings.csv"
SOURCE_XLSX = ASSIGNMENT_DIR / "iomt_data" / "patients_data_with_alerts.xlsx"
STREAM_INPUT_DIR = ASSIGNMENT_DIR / "stream_input"
WINDOW_OUT_DIR = ASSIGNMENT_DIR / "stream_window_out"
CHECKPOINT_DIR = ASSIGNMENT_DIR / "checkpoints"
ALERTS_DIR = ASSIGNMENT_DIR / "alerts"


def event_time_for_tick(tick: int) -> _dt.datetime:
    """Synthesized event timestamp for the tick-th reading of any patient."""
    return BASE_TIME + _dt.timedelta(seconds=tick * TICK_SECONDS)


def flush_event_time(n_ticks: int) -> _dt.datetime:
    """Far-future timestamp for the watermark-flush sentinel tick."""
    return event_time_for_tick(n_ticks - 1) + _dt.timedelta(seconds=FLUSH_GAP_SECONDS)
