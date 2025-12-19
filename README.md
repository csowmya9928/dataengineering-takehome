# Data Engineering Take‑Home (Medium)

## Goal
Build a **daily ingestion + cleaning + validation + quarantine + metrics** pipeline for messy product data.

You will process daily partitions under `data/raw/ingest_date=YYYY-MM-DD/` and produce:
- Cleaned, partitioned outputs (Parquet)
- Quarantine outputs (Parquet) with `reject_reason`
- Daily volume + data quality metrics (Parquet)
- Validation + alert reports (JSON)

## Overview 
This project implements a daily data ingestion and data quality pipeline for messy product event data.
For each ingest_date, the pipeline reads raw CSV files, normalizes and validates records, separates
clean and quarantined data, detects duplicates, and generates daily data quality and performance
metrics. The outputs are written in partitioned Parquet format along with JSON validation and alert
reports to support monitoring and downstream analytics.

## How to Run

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

## Assumptions

### Timestamp assumptions

- Timestamps may come in mixed formats/timezones; parsing uses a tolerant parser and converts everything to UTC.
- When timezone info is missing, assuming it is UTC

### Deduplication policy assumptions

Customers : 
- one row per customer_id after dedupe. Prefer the “best” record by:
  valid email present, created_at parseable, latest created_at (tie-breaker)

Events :
- Duplicate full rows can be dropped safely.
- For duplicate event_id, keep the “best” record by completeness (has timestamp, has customer_id, has event_type, has platform, has session_id, has duration, then latest event_time)

Orders :
- Duplicate full rows can be dropped safely.
- For duplicate order_id, keep the “best” record by completeness order_id, has_order_time, has_customer, has_amount, has_currency,
  has_status, order_time_utc
  
### Validation and Quarantine assumptions

- Referential integrity is enforced: `customer_id` in events,orders must exist in `customers dataset`.

### Numeric rules assumptions

duration_ms is milliseconds:

  - must be >= 0
  - must be <= MAX_DURATION_MS (24h = 86,400,000 ms)

amount:

 - must be numeric
 - must be >= 0 for all statuses
 - paid → amount > 0
 - failed → amount can be 0
 - refunded/chargeback → amount > 0 (represents original amount)
 
### Metrics assumptions

- Metrics are computed on clean data only (especially p50/p95), so outliers/quarantined rows don’t skew monitoring.

### Partial-load detection assumptions

- Hourly coverage uses event_time_utc bucketed to hours, alert if more than 4 hours are missing.
- Volume-drop heuristic compares today vs trailing median of last N = 7 available days,  alert if drop > 50%. If fewer than N prior days exist, skip this alert.

### Incremental/idempotency assumptions

- Rerunning a date overwrites that date’s outputs (clean/quarantine/reports/metrics) so results are idempotent.
- Metrics files are written partitioned by ingest_date so reruns don’t double-count.

## Outputs
<img width="666" height="1430" alt="image" src="https://github.com/user-attachments/assets/db2ca33f-7c9e-4d56-a507-fac6329c6385" />



