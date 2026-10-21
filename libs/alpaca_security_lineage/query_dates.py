"""Select metadata query dates from supplied download descriptors."""

from datetime import datetime, timezone
from typing import Any

from .settings import EARLIEST_BAR_DATE


def local_query_dates(local_report: dict[str, Any]) -> tuple[str, str]:
    request_start_dates = []
    asof_dates = []
    for download in local_report["downloads"]:
        request_from = download.get("request_from")
        asof = download.get("asof")
        if isinstance(request_from, str) and len(request_from) >= 10:
            request_start_dates.append(request_from[:10])
        if isinstance(asof, str) and len(asof) >= 10:
            asof_dates.append(asof[:10])
    start_date = min(request_start_dates, default=EARLIEST_BAR_DATE)
    end_date = max(asof_dates, default=datetime.now(timezone.utc).date().isoformat())
    return start_date, end_date
