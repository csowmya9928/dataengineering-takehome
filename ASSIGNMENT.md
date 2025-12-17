# Take‑Home Assignment — Data Engineering (Medium)

## Scenario
You ingest 3 raw feeds daily:
- customers (snapshot-ish, may contain duplicates)
- events (append-only, highest volume)
- orders (append-only)

Raw data is messy: invalid timestamps, duplicates, orphan records, inconsistent categories, outliers,
and sometimes partial loads (missing hours).

Your task: build a robust Python pipeline that produces clean datasets and trustworthy metrics.

---

## Inputs
Daily raw files live in:
- `data/raw/ingest_date=YYYY-MM-DD/customers_raw.csv`
- `data/raw/ingest_date=YYYY-MM-DD/events_raw.csv`
- `data/raw/ingest_date=YYYY-MM-DD/orders_raw.csv`

Expected raw columns:

### customers_raw.csv
- customer_id (string)
- email (string, may be invalid)
- created_at (string datetime; mixed formats)
- country (string; inconsistent)
- status (string: active/inactive/banned; inconsistent)
- ingest_date (string date; may be missing or wrong)

### events_raw.csv
- event_id (string; may duplicate)
- customer_id (string; may be orphan)
- event_time (string datetime; mixed formats/timezones/invalid)
- event_type (string; inconsistent; unknowns)
- platform (string: ios/android/web; inconsistent)
- session_id (string; may be missing)
- duration_ms (numeric; may be negative / absurd)
- ingest_date (string date)

### orders_raw.csv
- order_id (string; may duplicate)
- customer_id (string; may be orphan)
- order_time (string datetime; mixed formats/invalid)
- amount (numeric; may be missing/negative)
- currency (string e.g. USD/usd/$; inconsistent)
- status (string: paid/failed/refunded/chargeback; inconsistent)
- ingest_date (string date)

---

## Requirements

### 1) Cleaning + standardization
Implement cleaning to:
- parse timestamps to UTC (or documented consistent timezone)
- coerce numerics
- standardize canonical categories (country/status/platform/currency/order_status/event_type)
- dedupe with **explicit rules**:
  - customers: one row per customer_id after dedupe
  - events/orders: define what to do with duplicate IDs and/or duplicate full rows

### 2) Validation + quarantine
Implement validation checks and split outputs into:
- **clean** records
- **quarantine** records with `reject_reason` (one or more reasons)

Minimum validation rules:
- required columns present
- timestamps parseable
- duration_ms >= 0 and <= a reasonable max
- referential integrity: events/orders must reference existing customer_id
- currency known after normalization
- amount rules consistent with order status (define your policy)
- ingest_date matches partition date (if present)

Produce:
- `reports/ingest_date=YYYY-MM-DD/validation_report.json` containing counts:
  - total / clean / quarantine per dataset
  - quarantine counts by reject_reason
  - duplicates detected (id-based and full-row)

### 3) Daily metrics (volume + quality)
Compute daily metrics per ingest_date:
- Volume: total/clean/quarantine counts for each dataset
- Breakdown: events by event_type/platform; orders by status
- Distinct customers active in events/orders
- Quality:
  - invalid timestamp rate
  - orphan customer_id rate
  - duplicate rates
  - null rates for key columns
  - numeric distribution sanity (p50/p95 for duration_ms, amount)

### 4) Partial-load detection
Add at least one simple heuristic (no ML):
- hourly coverage: compute events per hour; flag if > X hours have 0 events
AND/OR
- volume drop: compare today’s event volume to trailing N-day median; flag if drop > Y%

Write alerts to:
- `reports/ingest_date=YYYY-MM-DD/alerts.json`

### 5) Incremental + idempotent
Support:
- `--date YYYY-MM-DD` or `--start/--end`
- idempotent reruns: reprocessing a date should not duplicate metrics or outputs

### 6) Tests
Add at least **8 tests** with pytest (starter includes a few).

---

## Deliverables
Return a zip or repo containing:
- your updated `src/` code
- updated/added tests in `tests/`
- README updates (assumptions + how to run)
- example reports/outputs (can be generated)

---

## Notes
- Local-friendly performance matters: avoid reading massive event files entirely into memory if possible.
- You may use pandas/polars/pyarrow; keep it simple and reproducible.
