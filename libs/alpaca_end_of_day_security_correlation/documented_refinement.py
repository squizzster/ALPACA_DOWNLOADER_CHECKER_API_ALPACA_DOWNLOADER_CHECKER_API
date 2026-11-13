"""Compatibility name for a retired researched-chronology execution path.

Completed dated assignments remain ordinary publication data. New inference
uses provider observations through the registered generic rule engine.
"""


def refine(inputs, reports, plan):
    if plan:
        raise ValueError("researched chronology refinement is inadmissible for new inference")
    return reports
