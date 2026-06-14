# Module 6 Assignment — Real-Time Stream Processing (Spark Structured Streaming)

Repository for Ontario Tech course **ENGR 5785G — Real-Time Data Analytics IoT**.

Student: Vitor Brandao Raposo

Student ID: 101011969

Date: 06/2026

---

## Scenario B — Hospital Patient Monitoring (IoMT)

> Detect **sustained** abnormal heart rates, not single spikes, across ICU patient streams.

| Requirement (from the assignment PDF)                          | This implementation                                                                  |
| -------------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| Window: **tumbling 2 min**                                     | `window("event_time", "2 minutes")`                                                  |
| Compute **average heart rate per patient per window**          | `groupBy(window, patient_id).agg(avg("heart_rate"))`                                 |
| Flag patients **exceeding 100 bpm in two consecutive windows** | per-patient consecutive-breach tracking in `foreachBatch`                            |
| Alert: **clinical alert with patient ID**                      | `*** CLINICAL ALERT *** patient N ...` in the console + `alerts/clinical_alerts.csv` |
| `readStream` with a **watched directory**                      | `spark.readStream.json("stream_input/")`                                             |
| **Window aggregation with `withWatermark`**                    | `withWatermark("event_time", "4 minutes")`                                           |
| **Alert condition as a filtered output stream**                | `avg_hr > 100` filter, surfaced as the alert stream                                  |

Spark runs **inside Docker** (no local Java/winutils needed), matching the repo's
"infrastructure is Docker-managed" convention.

---

## First-Time Setup (Windows)

You need two things on your PATH: **Docker Desktop** (runs the Spark job) and
[**uv**](https://docs.astral.sh/uv/) (creates the Python environment for the one-time
data-prep script). You do **not** need a local Python or Java install — `uv`
fetches a managed Python, and Spark runs inside Docker.

```powershell
# from the project root (realtime_stream_processing_assignment/)
uv venv                                    # creates .venv/ (downloads CPython if needed)
.venv\Scripts\Activate.ps1                 # activate it
uv pip install -r requirements.txt         # host-side deps: pandas + openpyxl only
```

> If `.venv\Scripts\Activate.ps1` is blocked by execution policy, run
> `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass` first, or skip activation
> and call the interpreter directly: `.venv\Scripts\python.exe -m src.prepare_data`.

Place the Kaggle source dataset at `iomt_data/patients_data_with_alerts.xlsx`
(see [Dataset](#dataset)) before running step 1 below.

## Quick Start (Windows)

> **Prefer a notebook?** [`run_assignment.ipynb`](run_assignment.ipynb) runs all of
> these steps top to bottom — setup, dataset, Spark, producer, and the alert output —
> with the wait-for-ready and runtime-cleanup handled for you.

```powershell
# from the project root, with .venv active (see First-Time Setup above)
.venv\Scripts\Activate.ps1

# 1. Build the streamable dataset from the Kaggle xlsx (one-time)
python -m src.prepare_data                # -> iomt_data/icu_readings.csv

# 2. Start the Spark Structured Streaming job (waits for files)
docker compose up -d
docker compose logs -f spark              # watch this window — screenshot the CLINICAL ALERT lines here

# 3. In a second terminal, feed the stream
python -m src.producer                    # writes timestamped JSON ticks into stream_input/

# 4. Tear down
docker compose down
```

### Re-running

Spark records every input file it has already ingested in its checkpoint, and the
producer reuses the same `tick_*.json` filenames each run. So **before every fresh
run you must wipe the runtime directories**, otherwise Spark resumes from the
checkpoint, skips the re-emitted files, and prints no new windows or alerts:

```powershell
docker compose down
Remove-Item checkpoints, stream_input, stream_window_out, alerts -Recurse -Force -ErrorAction SilentlyContinue
```

Then start Spark and **wait for the `[spark] watching …` line** (`docker compose up -d`,
`docker compose logs -f spark`) _before_ running the producer.

---

## Repository Structure

```text
realtime_stream_processing_assignment/
├── StreamProcessing_Assignment.pdf   ← Assignment spec
├── docker-compose.yml                ← Spark job runner (apache spark:3.5.3, local mode)
├── requirements.txt                  ← host deps: pandas, openpyxl (data prep only)
├── run_assignment.ipynb              ← runs the whole pipeline end to end (optional)
├── .venv/                            ← host Python env (created by `uv venv`, gitignored)
├── iomt_data/
│   ├── patients_data_with_alerts.xlsx ← Kaggle source (gitignored, not submitted)
│   └── icu_readings.csv               ← produced by prepare_data.py (gitignored)
├── src/
│   ├── config.py                     ← shared constants + the canonical event-time scheme
│   ├── prepare_data.py               ← xlsx → streamable CSV (round-robin onto ICU pool)
│   ├── producer.py                   ← writes timestamped JSON ticks into stream_input/
│   └── streaming_job.py              ← the Spark Structured Streaming job
├── stream_input/                     ← watched directory (gitignored, runtime)
├── checkpoints/  stream_window_out/  ← Spark state/output (gitignored, runtime)
├── alerts/clinical_alerts.csv        ← fired alerts (gitignored, runtime)
└── README.md                         ← this file
```

## Written Explanation

A short explanation answering the two required questions: **why this window type**,
and **where the pipeline requires state**.

**Why a tumbling window.** Scenario B asks for the average heart rate _per patient
per window_ and an alert on _two consecutive windows_. A **tumbling** (fixed,
non-overlapping) 2-minute window gives each patient exactly **one** average per
interval, so "two consecutive windows" maps cleanly to two adjacent, disjoint
intervals (`start₂ == start₁ + 2 min`). A sliding window would reuse the same
readings across overlapping windows, double-counting samples and making
"consecutive" ambiguous — inflating false alerts for what is meant to be a
_sustained_ signal. Tumbling is also exactly what the scenario prescribes.

**Where the pipeline requires state.** Two places:

1. **Windowed-aggregation state** (Spark-managed, checkpointed). Spark holds a
   running `avg(heart_rate)` for every `(patient_id, 2-min window)` key until the
   **watermark** (`event_time − 4 min`) passes the window's end, then append mode
   emits that window's final average once and evicts its state. The watermark is
   what bounds this state so it cannot grow without limit.

2. **Consecutive-breach state** (application-level). Detecting _two consecutive_
   breaching windows means remembering, per patient, the start of its most recent
   breaching window — information that spans windows and micro-batches, so it isn't
   expressible by a single windowed aggregation. The `foreachBatch` handler in
   [src/streaming_job.py](src/streaming_job.py) keeps a
   `patient_id → last_breaching_window_start` dictionary and fires the alert when
   the current breaching window is adjacent to the stored one.

---

## Finalizing the last windows

In append mode a window is only emitted once the watermark passes its end, so the
final real windows would otherwise stay buffered when the stream stops. To force
them out, the producer emits one far-future, below-threshold **"flush" tick**
(`src/config.py`) that advances the watermark past every real window — making Spark
finalize and print them all. The flush reading is below the 100 bpm threshold, so
it never produces an alert.
