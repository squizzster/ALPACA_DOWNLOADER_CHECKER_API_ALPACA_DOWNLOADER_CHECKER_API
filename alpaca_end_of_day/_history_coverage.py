"""Carry accepted contributing-source membership without ranking observations."""


def coverage_sources(coverage):
    """Legacy coverage selectors already establish their source membership.

    Consume their keys only. Subset/overlap relationships no longer select a
    numerical source and must not be reconstructed from retained timestamps.
    """
    if coverage.get("version") == 2:
        return set(coverage["sources"])
    if "version" in coverage:
        raise ValueError("unsupported contributing-source projection")
    return set(coverage["subsets"])


def extend_coverage(sources, previous=None):
    accepted = coverage_sources(previous) if previous is not None else set()
    return {"version": 2, "sources": sorted(accepted | set(sources))}
