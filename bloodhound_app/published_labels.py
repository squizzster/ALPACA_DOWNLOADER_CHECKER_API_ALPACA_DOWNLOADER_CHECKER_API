"""Project completed observed-label ranges without repeating ownership decisions."""


def ticker_labels(ranges, sessions):
    selected = [(item["first_session"], item["last_session"], item["ticker"]) for item in ranges]
    symbols = []
    for session in sessions:
        candidates = {ticker for start, end, ticker in selected if start <= session <= end}
        symbols.append(next(iter(candidates)) if len(candidates) == 1 else "")
    return symbols
