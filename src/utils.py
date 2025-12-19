\
from __future__ import annotations

import re
import pandas as pd
from datetime import datetime, timezone
from dateutil import parser
from difflib import SequenceMatcher

CANON_CURRENCY = {
    "usd": "USD",
    "$": "USD",
    "us$": "USD",
    "eur": "EUR",
    "€": "EUR",
    "‚Ç¨" : "EUR",
}

CANON_PLATFORM = {
    "ios": "ios",
    "iphone": "ios",
    "android": "android",
    "and": "android",
    "web": "web",
    "browser": "web",
}

CANON_COUNTRY = {
    "UNITED STATES": "US",
    "USA": "US",
    "US": "US",

    "UNITED KINGDOM": "GB",
    "UK": "GB",
    "GB": "GB",
    "GREAT BRITAIN": "GB",

    "INDIA": "IN",
    "IN": "IN",

    "N/A": None,
    "NA": None,
    "": None,
}

CANON_STATUS_CUSTOMER = ["active","inactive","banned"]

CANON_ORDER_STATUS = {
    "paid": "paid",
    "succeeded": "paid",
    "failed": "failed",
    "refunded": "refunded",
    "chargeback": "chargeback",
}

#Regex
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
CUSTOMER_ID_RE = re.compile(r"^c\d{5}$")
CUSTOMER_ID_EMBEDDED_RE = re.compile(r"(c\d{5})", re.IGNORECASE)

def is_valid_email(email: str | None) -> bool:
    if not email or not isinstance(email, str):
        return False
    return bool(EMAIL_RE.match(email.strip()))

def normalize_currency(x: str | None) -> str | None:
    """Normalize currency strings. Return canonical (e.g., USD) or None if unknown."""
    if x is None:
        return None
    s = str(x).strip().lower()
    s = s.replace(" ", "")
    return CANON_CURRENCY.get(s,None)

def normalize_platform(x: str | None) -> str | None:
    if x is None:
        return None
    s = str(x).strip().lower()
    return CANON_PLATFORM.get(s, s if s in {"ios","android","web"} else None)

def normalize_country(value: str | None) -> str | None:
    if value is None:
        return None
    v = str(value).strip().upper()
    return CANON_COUNTRY.get(v, None)

def similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()  # 0 - 1

def normalize_customer_status(value: str | None, threshold: float = 0.80) -> str | None:
    if value is None or pd.isna(value):
        return None

    s = str(value).strip().lower()
    if not s:
        return None

    best_label = None
    best_score = 0.0

    for label in CANON_STATUS_CUSTOMER:
        sc = similarity(s, label)
        if sc > best_score:
            best_score = sc
            best_label = label

    # Only accept if confident enough
    if best_score >= threshold:
        return best_label

    return None

def normalize_order_status(x: str | None) -> str | None:
    if x is None:
        return None
    s = str(x).strip().lower()
    return CANON_ORDER_STATUS.get(s,None)



def normalize_customer_id(value) -> str | None:
    if value is None:
        return None

    if isinstance(value, float) and value != value:  # NaN #reusable Python normalizer rather than a pandas-specific operation.
        return None

    s = str(value).strip().lower()
    if not s:
        return None

    if s in ("null", "none", "nan", "n/a", "na"):
        return None

    # Exact match
    if CUSTOMER_ID_RE.fullmatch(s):
        return s

    # Extract from messy string
    m = CUSTOMER_ID_EMBEDDED_RE.search(s)
    if m:
        candidate = m.group(1).lower()
        if CUSTOMER_ID_RE.fullmatch(candidate):
            return candidate

    return None


def parse_timestamp_to_utc(value: str | None) -> datetime | None:
    """
    Parse various timestamp formats into a UTC-aware datetime.

    Accepts:
      - ISO8601 strings (e.g., "2025-01-10T00:00:00Z", "2025-01-10T17:00:00-05:00")
      - mixed-format datetime strings (e.g., "01/07/2025 01:00:00", "Jan 10 2025 5:30PM")
      - epoch seconds (10+ digits, int/str)
      - epoch milliseconds (13+ digits, int/str)
      - YYYYMMDD numeric date codes (8 digits, int/str) -> interpreted as midnight UTC

    Rules:
      - If no timezone -> assume UTC
      - Return None for invalid timestamps
      - Reject ambiguous numeric timestamps (e.g., 202512, 2025010, 12345)
      - Accept years from 1900 to (current_year + 1)
    """

    # Quick rejection cases
    if value is None:
        return None
    if isinstance(value, float) and value != value:  # NaN
        return None

    current_year = datetime.now(timezone.utc).year
    min_year = 1900
    max_year = current_year + 1

    def year_valid(dt: datetime) -> bool:
        return min_year <= dt.year <= max_year

    s = str(value).strip()
    if not s:
        return None

    #Numeric-only inputs: YYYYMMDD vs epoch vs reject
    if s.isdigit():
        # YYYYMMDD date code
        if len(s) == 8:
            try:
                dt = datetime.strptime(s, "%Y%m%d").replace(tzinfo=timezone.utc)
                return dt if year_valid(dt) else None
            except Exception:
                return None

        # Reject ambiguous numeric timestamps like YYYYMM (6), 7 digits, 9 digits, etc.
        # Only allow epoch-style numbers (10+ digits).
        if len(s) < 10:
            return None

        # Epoch seconds (10 digits typical) or milliseconds (13 digits typical)
        try:
            ts_int = int(s)

            # milliseconds if very large
            if ts_int >= 1_000_000_000_000:
                ts = ts_int / 1000.0
            else:
                ts = float(ts_int)

            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            return dt if year_valid(dt) else None
        except Exception:
            return None

    #General string datetime parsing
    try:
        dt = parser.parse(s) #parses a date/time string into a datetime object

        # If no timezone info -> assume UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        dt = dt.astimezone(timezone.utc)
        return dt if year_valid(dt) else None
    except Exception:
        return None

def safe_float(x) -> float | None:
    try:
        if x is None:
            return None
        if isinstance(x, str) and x.strip() == "":
            return None
        v = float(x)
        if v != v:  # NaN
            return None
        return v
    except Exception:
        return None
