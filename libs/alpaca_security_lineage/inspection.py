from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from .alpaca_http import AlpacaIdentityClient, AlpacaIdentityRequestRateLimiter
from .lineage import build_identity_report
from .local_downloads import inspect_local_downloads
from .settings import REPORT_VERSION, SYMBOL_PATTERN


def inspect_alpaca_security_lineage(
    ticker: str,
    api_key_id: str,
    api_secret_key: str,
    *,
    repository_root: Path | None = None,
    client_factory: Callable[[str, str], AlpacaIdentityClient] = AlpacaIdentityClient,
    generated_at: datetime | None = None,
    request_rate_limiter: AlpacaIdentityRequestRateLimiter | None = None,
) -> dict:
    """Return local and direct-API identity evidence without writing any files."""
    symbol = ticker.upper()
    if not SYMBOL_PATTERN.fullmatch(symbol):
        raise ValueError(f"invalid ticker: {ticker!r}")

    local_report = inspect_local_downloads(repository_root or Path.cwd(), symbol)
    if request_rate_limiter is None:
        client = client_factory(api_key_id, api_secret_key)
    else:
        client = client_factory(
            api_key_id,
            api_secret_key,
            rate_limiter=request_rate_limiter,
        )
    lineage_report = build_identity_report(symbol, local_report, client)
    report_time = generated_at or datetime.now(timezone.utc)
    return {
        "report_version": REPORT_VERSION,
        "generated_at": report_time.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "symbol": symbol,
        "local": local_report,
        "lineage": lineage_report,
    }
