"""Reject the removed factual-candidate diagnostic interface.

Historical reports remain inspectable as data. New diagnostic inference selects
registered generic rules and parameters; research assertions are never inputs.
"""


def evaluate(*args, **kwargs):
    raise ValueError("fact-backed research candidates are not supported; use registered generic rules")
