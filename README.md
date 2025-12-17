# Data Engineering Take‑Home (Medium)

## Goal
Build a **daily ingestion + cleaning + validation + quarantine + metrics** pipeline for messy product data.

You will process daily partitions under `data/raw/ingest_date=YYYY-MM-DD/` and produce:
- Cleaned, partitioned outputs (Parquet)
- Quarantine outputs (Parquet) with `reject_reason`
- Daily volume + data quality metrics (Parquet)
- Validation + alert reports (JSON)

This repo includes **messy sample data** for a few dates and a **data generator** to create more.

---

## Quick start

### 1) Setup (Python 3.10+ recommended)
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Run for one day
```bash
python -m src.pipeline --data_dir data/raw --out_dir data --date 2025-12-10
```

### 3) Run for a range
```bash
python -m src.pipeline --data_dir data/raw --out_dir data --start 2025-12-10 --end 2025-12-12
```

### 4) Run tests
```bash
pytest -q
```

> Note: The starter implementation contains TODOs. Some tests will initially fail until you implement missing logic.

---

## What you need to implement
See **ASSIGNMENT.md** for full requirements and acceptance criteria.

---

## Folder structure
```
data/
  raw/ingest_date=YYYY-MM-DD/*.csv
  clean/{customers,events,orders}/ingest_date=YYYY-MM-DD/*.parquet
  quarantine/{customers,events,orders}/ingest_date=YYYY-MM-DD/*.parquet
  metrics/daily_metrics.parquet
  metrics/hourly_events.parquet
reports/ingest_date=YYYY-MM-DD/{validation_report.json,alerts.json}
src/  (pipeline code)
tests/ (pytest)
scripts/generate_data.py  (generate more messy data)
```
