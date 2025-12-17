\
from __future__ import annotations

import pandas as pd

def detect_partial_load(ingest_date: str,
                        hourly_events: pd.DataFrame,
                        daily_metrics_history: pd.DataFrame | None) -> dict:
    """
    Return alerts dict for ingest_date.

    TODO: Candidate: implement partial load heuristics:
      - missing hour coverage (e.g., >4 hours with 0 events)
      - large volume drops vs trailing median
    """
    alerts = {"ingest_date": ingest_date, "flags": []}

    # Starter heuristic stub (always empty)
    return alerts
