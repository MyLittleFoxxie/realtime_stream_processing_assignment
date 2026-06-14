"""One-time prep: turn the cross-sectional Kaggle xlsx into a streamable CSV.

The source `patients_data_with_alerts.xlsx` has 50,000 rows, each a *unique*
patient with a single reading and no timestamp. Scenario B needs patients that
recur over time, so we remap every reading round-robin onto a small pool of
ICU patient IDs (config.POOL_SIZE). The producer later replays this CSV,
synthesizing event timestamps, so each pooled patient gets a realistic
time series of heart-rate readings.

Run:  python -m src.prepare_data
"""
from __future__ import annotations

import pandas as pd

from src import config

SRC_PATIENT_COL = "Patient Number"
SRC_HR_COL = "Heart Rate (bpm)"


def main() -> None:
    if not config.SOURCE_XLSX.exists():
        raise FileNotFoundError(
            f"Source dataset not found: {config.SOURCE_XLSX}\n"
            "Download the Kaggle IoMT dataset and place it there (see README)."
        )

    print(f"[prepare] reading {config.SOURCE_XLSX} ...")
    df = pd.read_excel(config.SOURCE_XLSX, usecols=[SRC_PATIENT_COL, SRC_HR_COL])
    df = df.dropna(subset=[SRC_HR_COL]).reset_index(drop=True)
    print(f"[prepare] {len(df)} source readings loaded")

    # Round-robin remap onto the ICU pool: row i -> patient (i % POOL_SIZE) + 1.
    out = pd.DataFrame(
        {
            "patient_id": (df.index % config.POOL_SIZE) + 1,
            "heart_rate": df[SRC_HR_COL].astype(float).round(2),
        }
    )

    config.READINGS_CSV.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(config.READINGS_CSV, index=False)

    print(f"[prepare] wrote {len(out)} readings -> {config.READINGS_CSV}")
    print(f"[prepare] patient pool size: {out['patient_id'].nunique()}")
    print(f"[prepare] HR mean={out['heart_rate'].mean():.1f} bpm, "
          f">{config.HR_THRESHOLD:.0f} bpm: {(out['heart_rate'] > config.HR_THRESHOLD).mean() * 100:.1f}%")


if __name__ == "__main__":
    main()
