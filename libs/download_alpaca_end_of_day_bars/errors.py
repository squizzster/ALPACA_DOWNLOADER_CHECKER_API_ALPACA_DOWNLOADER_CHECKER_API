class AlpacaInvalidTickerCodeError(SystemExit):
    """An exact Alpaca HTTP response says the submitted ticker code is invalid."""

    reason_code = "invalid_ticker_code"

    def __init__(
        self,
        symbol: str,
        adjustment: str,
        http_status: int,
        response_body: str,
    ) -> None:
        self.symbol = symbol
        self.adjustment = adjustment
        self.http_status = http_status
        self.response_body = response_body
        super().__init__(f"{adjustment}: Alpaca returned HTTP {http_status}: {response_body}")
