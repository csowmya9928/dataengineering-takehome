\
from __future__ import annotations

import os
import random
import string
from datetime import datetime, timedelta, timezone
import pandas as pd
import numpy as np

def _rand_id(prefix: str, n=8) -> str:
    return prefix + "".join(random.choices(string.ascii_lowercase + string.digits, k=n))

def make_day(ingest_date: str, out_dir: str, *, n_customers=200, n_events=5000, n_orders=400, partial_load=False, seed=42):
    random.seed(seed + int(ingest_date.replace("-","")))
    np.random.seed(seed + int(ingest_date.replace("-","")))

    day_dir = os.path.join(out_dir, f"ingest_date={ingest_date}")
    os.makedirs(day_dir, exist_ok=True)

    # Customers
    countries = ["US","usa","United States","IN","india","GB","uk","BR","N/A",None]
    statuses = ["active","ACTIVE","inactive","banned","",None,"actve"]
    rows = []
    base_created = datetime(2025, 1, 1, tzinfo=timezone.utc)
    for i in range(n_customers):
        cid = f"c{i:05d}"
        created = base_created + timedelta(days=random.randint(0, 300), hours=random.randint(0,23))
        # messy formats
        created_str = created.strftime("%Y-%m-%d %H:%M:%S") if random.random() < 0.6 else created.isoformat()
        email = f"user{i}@example.com" if random.random() < 0.9 else f"user{i}example.com"
        rows.append({
            "customer_id": cid,
            "email": email,
            "created_at": created_str,
            "country": random.choice(countries),
            "status": random.choice(statuses),
            "ingest_date": ingest_date if random.random() < 0.9 else "2099-01-01"
        })
        # duplicates
        if random.random() < 0.05:
            rows.append(rows[-1] | {"created_at": "not a date"})
    customers = pd.DataFrame(rows)
    customers.to_csv(os.path.join(day_dir, "customers_raw.csv"), index=False)

    # Events
    event_types = ["login","feature_use","error","Logout","FEATURE_USE","",None,"paywall_view"]
    platforms = ["ios","android","web","iPhone","browser","AND","",None]
    events = []
    start = datetime.strptime(ingest_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    # If partial_load, skip 6 consecutive hours
    missing_hours = set()
    if partial_load:
        h0 = random.randint(0, 10)
        missing_hours = set(range(h0, h0+6))

    for i in range(n_events):
        eid = _rand_id("e")
        cid = f"c{random.randint(0, n_customers-1):05d}"
        # orphans
        if random.random() < 0.02:
            cid = "unknown_" + cid

        hr = random.randint(0, 23)
        if hr in missing_hours:
            hr = (hr + 7) % 24
        ts = start + timedelta(hours=hr, minutes=random.randint(0,59), seconds=random.randint(0,59))
        # messy timestamp formats/timezones
        if random.random() < 0.6:
            ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")
        elif random.random() < 0.85:
            ts_str = ts.astimezone(timezone(timedelta(hours=-5))).isoformat()  # -05:00
        else:
            ts_str = "bad-ts"

        dur = int(np.random.exponential(scale=120000))  # ms
        if random.random() < 0.01:
            dur = -dur
        if random.random() < 0.005:
            dur = 999999999  # absurd

        events.append({
            "event_id": eid,
            "customer_id": cid,
            "event_time": ts_str,
            "event_type": random.choice(event_types),
            "platform": random.choice(platforms),
            "session_id": _rand_id("s") if random.random() < 0.8 else "",
            "duration_ms": dur,
            "ingest_date": ingest_date
        })
        # duplicate IDs
        if random.random() < 0.01:
            events.append(events[-1])
    pd.DataFrame(events).to_csv(os.path.join(day_dir, "events_raw.csv"), index=False)

    # Orders
    currencies = ["USD","usd","$","EUR","€","???",None]
    order_status = ["paid","PAID","failed","refunded","chargeback","",None,"succeeded"]
    orders = []
    for i in range(n_orders):
        oid = _rand_id("o")
        cid = f"c{random.randint(0, n_customers-1):05d}"
        if random.random() < 0.03:
            cid = "unknown_" + cid

        ts = start + timedelta(hours=random.randint(0,23), minutes=random.randint(0,59))
        ts_str = ts.isoformat() if random.random() < 0.8 else "not-a-time"

        amt = round(float(np.random.gamma(shape=2.0, scale=20.0)), 2)
        if random.random() < 0.02:
            amt = -amt
        if random.random() < 0.02:
            amt = None

        orders.append({
            "order_id": oid,
            "customer_id": cid,
            "order_time": ts_str,
            "amount": amt,
            "currency": random.choice(currencies),
            "status": random.choice(order_status),
            "ingest_date": ingest_date
        })
        if random.random() < 0.01:
            orders.append(orders[-1])
    pd.DataFrame(orders).to_csv(os.path.join(day_dir, "orders_raw.csv"), index=False)

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--out_dir", default="data/raw")
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--partial_load_every", type=int, default=3, help="every Nth day is partial load (0 disables)")
    args = ap.parse_args()

    s = datetime.strptime(args.start, "%Y-%m-%d").date()
    e = datetime.strptime(args.end, "%Y-%m-%d").date()
    cur = s
    idx = 0
    while cur <= e:
        d = cur.strftime("%Y-%m-%d")
        partial = (args.partial_load_every and idx % args.partial_load_every == 0)
        make_day(d, args.out_dir, partial_load=partial, seed=args.seed)
        cur += timedelta(days=1)
        idx += 1

if __name__ == "__main__":
    main()
