"""Scenario B — Spark Structured Streaming: ICU heart-rate monitoring.

Pipeline
--------
1. readStream JSON from the watched directory stream_input/.
2. withWatermark on event_time, then a TUMBLING 2-minute window aggregation
   computing the average heart rate per patient per window  ── STATE #1 (Spark-managed,
   checkpointed; bounded by the watermark).
3. Append output mode -> foreachBatch, which receives each window exactly once
   after the watermark finalizes it. There we:
     - print the per-window average,
     - apply the alert FILTER (avg_hr > 100 bpm),
     - track each patient's most recent breaching window  ── STATE #2 (application-level)
       to fire a CLINICAL ALERT when a patient breaches in TWO CONSECUTIVE windows,
     - append fired alerts to alerts/clinical_alerts.csv.

Runs via spark-submit inside the Docker container (see docker-compose.yml).
"""
from __future__ import annotations

import csv
import datetime as dt
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StructField,
    StructType,
    TimestampType,
)

from src import config

READING_SCHEMA = StructType(
    [
        StructField("patient_id", IntegerType()),
        StructField("heart_rate", DoubleType()),
        StructField("event_time", TimestampType()),
    ]
)

# Application-level state (STATE #2): patient_id -> start of its most recent
# breaching (avg_hr > threshold) window. Lets us detect adjacency across batches.
_last_breach_start: dict[int, dt.datetime] = {}


def _ensure_alert_header(path: Path) -> None:
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", newline="") as fh:
            csv.writer(fh).writerow(
                ["patient_id", "prev_window_start", "curr_window_start", "avg_hr"]
            )


def process_window_batch(batch_df, batch_id: int) -> None:
    """foreachBatch handler: each row is one finalized (patient, window) average."""
    rows = batch_df.collect()
    if not rows:
        return

    # Order by patient then window so consecutive-window detection is correct.
    rows.sort(key=lambda r: (r["patient_id"], r["window_start"]))
    alerts_csv = config.ALERTS_DIR / "clinical_alerts.csv"
    _ensure_alert_header(alerts_csv)

    print(f"\n===== batch {batch_id}: {len(rows)} finalized window(s) =====", flush=True)
    fired = []
    for r in rows:
        pid = r["patient_id"]
        start: dt.datetime = r["window_start"]
        avg_hr = r["avg_hr"]
        breach = avg_hr > config.HR_THRESHOLD                       # the alert FILTER
        flag = "  <-- HIGH" if breach else ""
        print(
            f"  patient {pid:>2} | window {start:%H:%M:%S}-{r['window_end']:%H:%M:%S} "
            f"| avg HR {avg_hr:6.2f} bpm{flag}",
            flush=True,
        )

        if breach:
            prev = _last_breach_start.get(pid)
            consecutive = prev is not None and (
                start - prev == dt.timedelta(seconds=config.WINDOW_SECONDS)
            )
            if consecutive:
                print(
                    f"  *** CLINICAL ALERT *** patient {pid}: avg HR > {config.HR_THRESHOLD:.0f} bpm "
                    f"in two consecutive windows ({prev:%H:%M}-{start:%H:%M}), latest avg {avg_hr:.1f} bpm",
                    flush=True,
                )
                fired.append((pid, prev, start, round(avg_hr, 2)))
            _last_breach_start[pid] = start

    if fired:
        with open(alerts_csv, "a", newline="") as fh:
            w = csv.writer(fh)
            for pid, prev, start, avg_hr in fired:
                w.writerow([pid, prev.isoformat(), start.isoformat(), avg_hr])


def main() -> None:
    spark = (
        SparkSession.builder.appName("ICU-HeartRate-Monitor")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # readStream requires the watched directory to exist at startup.
    for d in (config.STREAM_INPUT_DIR, config.CHECKPOINT_DIR, config.ALERTS_DIR):
        d.mkdir(parents=True, exist_ok=True)

    raw = (
        spark.readStream.schema(READING_SCHEMA)
        .json(str(config.STREAM_INPUT_DIR))
    )

    # STATE #1: windowed average heart rate per patient (tumbling 2 min + watermark).
    windowed = (
        raw.withWatermark("event_time", config.WATERMARK_DELAY)
        .groupBy(
            F.window("event_time", config.WINDOW_DURATION).alias("w"),
            F.col("patient_id"),
        )
        .agg(F.avg("heart_rate").alias("avg_hr"), F.count("*").alias("n"))
        .select(
            "patient_id",
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "avg_hr",
            "n",
        )
    )

    query = (
        windowed.writeStream.outputMode("append")
        .foreachBatch(process_window_batch)
        .option("checkpointLocation", str(config.CHECKPOINT_DIR / "window_agg"))
        .trigger(processingTime="5 seconds")
        .start()
    )

    print(f"[spark] watching {config.STREAM_INPUT_DIR} | window={config.WINDOW_DURATION} "
          f"watermark={config.WATERMARK_DELAY} threshold={config.HR_THRESHOLD:.0f} bpm", flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()
