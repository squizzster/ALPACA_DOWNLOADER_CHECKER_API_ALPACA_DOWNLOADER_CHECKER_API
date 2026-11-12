"""Carry completed serving preparation under exact operation bindings."""

from libs.alpaca_lineage_build.recovery_handoffs import recall, remember

from .contract import policy


def scope(group, previous, rebuild):
    reader = getattr(group, "_reader", None)
    if reader is None or not hasattr(group, "references"):
        return None
    references = group.references
    if any("decision_generation" not in value for value in previous):
        return None
    return reader.store, (
        tuple(sorted(policy().items())),
        tuple(sorted(references["lineage"].items())),
        tuple(sorted(references["pricing"].items())),
        tuple(sorted(value["decision_generation"] for value in previous)),
        rebuild,
    )


def selected(binding):
    return recall(binding[0], "serving_preparation", binding[1]) if binding is not None else None


def completed(binding, value):
    if binding is not None:
        remember(binding[0], "serving_preparation", binding[1], value)
    return value


def histories(binding, group, public_id, prefix):
    if binding is None:
        return group.serving_histories(public_id, prefix=prefix)
    key = binding[1], public_id, prefix is not None
    value = recall(binding[0], "serving_prepared_histories", key)
    if value is None:
        value = group.serving_histories(public_id, prefix=prefix)
        remember(binding[0], "serving_prepared_histories", key, value)
    return value


def revised(binding, group, public_id, prefix, work):
    from .source_revision import finish_revision

    if binding is None:
        return finish_revision(group, public_id, prefix, work)
    key = binding[1], public_id
    value = recall(binding[0], "serving_prepared_revision", key)
    if value is None:
        value = finish_revision(group, public_id, prefix, work)
        remember(binding[0], "serving_prepared_revision", key, value)
    return value
