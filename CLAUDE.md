# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A course assignment (Ontario Tech ENGR 5785G) implementing **Scenario B — Hospital
Patient Monitoring** with Spark Structured Streaming: detect *sustained* abnormal heart
rates (avg HR > 100 bpm in **two consecutive** 2-minute tumbling windows) per ICU patient.
The grading artifact is `src/streaming_job.py` plus a screenshot of the `CLINICAL ALERT`
console output. See `README.md` for the full assignment-requirement mapping and
`StreamProcessing_Assignment.pdf` for the spec.

## Commands

The Spark job runs **inside Docker** (image bundles Java + PySpark); host-side Python is
only for the one-time data prep. Full run sequence:

```powershell
uv venv                                  # one-time: create .venv/ (uv fetches CPython)
.venv\Scripts\Activate.ps1               # in-project venv
uv pip install -r requirements.txt       # host deps: pandas, openpyxl only (NOT pyspark)

python -m src.prepare_data               # xlsx -> iomt_data/icu_readings.csv (one-time)
docker compose up -d                      # start the streaming job (waits for files)
docker compose logs -f spark             # watch / screenshot CLINICAL ALERT lines here
python -m src.producer                   # in a 2nd terminal: feed stream_input/
docker compose down
```

There is no test suite or linter. Success is the `*** CLINICAL ALERT ***` lines appearing
in the Spark log (and rows in `alerts/clinical_alerts.csv`); a correct full run produces
167 alerts.

**Re-running requires wiping runtime state first.** Spark records every ingested input
file in its checkpoint, and the producer reuses the same `tick_*.json` filenames, so on a
second run Spark skips them and prints nothing. Before each fresh run: `docker compose
down`, then delete the gitignored runtime dirs `checkpoints/ stream_input/
stream_window_out/ alerts/`, then bring Spark up and wait for the `[spark] watching …`
line **before** starting the producer.

## Architecture

The whole system is built around **determinism**: the producer and the Spark job share the
same constants and timestamp scheme via `src/config.py`, so windowing behavior is
reproducible run to run.

- **`src/config.py`** — the single source of truth. Pure stdlib (no pandas/pyspark) so it
  imports identically on the host and in the container. Defines pool size, tick cadence,
  window/watermark/threshold constants, and the canonical timestamp functions
  (`event_time_for_tick`, `flush_event_time`). **Change windowing behavior here, not in
  the individual scripts**, or the producer and Spark job drift apart.

- **`src/prepare_data.py`** — one-time transform. The Kaggle xlsx is *cross-sectional*
  (50k unique patients, one reading each, no timestamps). This remaps readings
  **round-robin onto `POOL_SIZE` (30) ICU patient IDs** so patients recur over time,
  which is what makes "two consecutive windows" meaningful.

- **`src/producer.py`** — replays the CSV into `stream_input/` one JSON file per "tick"
  (one reading per patient), synthesizing event timestamps from `config`. Writes
  **atomically** (dotfile + `os.replace`) so Spark never reads a partial file; dotfiles
  are ignored by Spark's file source. Ends with a far-future, below-threshold **flush
  sentinel** tick that advances the watermark so append-mode finalizes every real window.

- **`src/streaming_job.py`** — the Spark job. Two distinct kinds of state:
  1. **Windowed-aggregation state** (Spark-managed, checkpointed, bounded by the
     `withWatermark` of 4 min): tumbling 2-min `avg(heart_rate)` per `(patient_id, window)`.
  2. **Consecutive-breach state** (application-level): a module-global
     `_last_breach_start: dict[patient_id -> last breaching window start]` inside the
     `foreachBatch` handler. This is *why* `foreachBatch` is used — detecting adjacency
     across micro-batches isn't expressible as a single windowed aggregation. The handler
     prints every finalized window, applies the `avg_hr > 100` filter, fires the alert when
     the current breaching window is exactly `WINDOW_SECONDS` after the stored one, and
     appends fired alerts to `alerts/clinical_alerts.csv`.

### Data flow
`xlsx → prepare_data → icu_readings.csv → producer → stream_input/*.json →
streaming_job (Spark) → console CLINICAL ALERTs + alerts/clinical_alerts.csv`

## Important constraints

- The Kaggle `*.xlsx` and derived `icu_readings.csv` are **gitignored and must not be
  committed** (submission rule). `prepare_data.py` regenerates the CSV.
- `docker-compose.yml` bind-mounts the whole project at `/app` and sets `PYTHONPATH=/app`,
  so all scripts use absolute `from src import config` imports — keep that package layout.
- Timestamps are anchored at the Unix epoch / `BASE_TIME` and computed in UTC
  (`spark.sql.session.timeZone=UTC`); tumbling windows floor to `WINDOW_SECONDS`
  boundaries. Don't introduce local-time conversions.
