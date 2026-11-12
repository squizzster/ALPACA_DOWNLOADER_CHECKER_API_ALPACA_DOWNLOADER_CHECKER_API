"""Read-only lineage research and visibility; production publication is separate."""

VERSION = 5
SCHEMA_VERSION = 1


def implementation_binding():
    from libs.alpaca_end_of_day_security_correlation.correlation_version import RUNTIME_VERSION
    from libs.alpaca_lineage_build.contract import RUNTIME_VERSION as LINEAGE_RUNTIME_VERSION
    from libs.alpaca_lineage_policy_lab.contract import LAB_VERSION

    return {
        "debug": VERSION,
        "policy_lab": LAB_VERSION,
        "correlation_runtime": RUNTIME_VERSION,
        "lineage_runtime": LINEAGE_RUNTIME_VERSION,
        "membership_refinement": "accepted_same_security_containing_component_v1",
    }


def accept_report(value, operation, *, allow_previous=False):
    binding = implementation_binding()
    bindings = [binding]
    current = tuple(binding[name] for name in ("debug", "policy_lab", "correlation_runtime", "lineage_runtime"))
    # This release consolidates the existing producers and their JSON transport.
    # Known completed comparisons remain applicable under the separate policy,
    # store, selector and generation checks. Future producer versions must make
    # their own explicit compatibility decision instead of inheriting this one.
    if allow_previous and current in {(3, 22, 95, 98), (4, 22, 96, 99), (5, 22, 96, 100)}:
        bindings.extend(
            {
                "debug": version,
                "policy_lab": 21,
                "correlation_runtime": 95,
                "membership_refinement": binding["membership_refinement"],
            }
            for version in (2, 3)
        )
        if current in {(4, 22, 96, 99), (5, 22, 96, 100)}:
            bindings.append(
                {
                    "debug": 3,
                    "policy_lab": 22,
                    "correlation_runtime": 95,
                    "lineage_runtime": 98,
                    "membership_refinement": binding["membership_refinement"],
                }
            )
        if current == (5, 22, 96, 100):
            bindings.append({**binding, "debug": 4, "lineage_runtime": 99})
    if (
        not isinstance(value, dict)
        or value.get("schema") != "alpaca.lineage.debug.report"
        or type(value.get("schema_version")) is not int
        or value["schema_version"] != SCHEMA_VERSION
        or type(value.get("debug_version")) is not int
        or value.get("operation") != operation
        or not any(
            value["debug_version"] == accepted["debug"] and value.get("implementation_binding") == accepted
            for accepted in bindings
        )
    ):
        raise ValueError("saved diagnostic report has an incompatible contract or research implementation")
