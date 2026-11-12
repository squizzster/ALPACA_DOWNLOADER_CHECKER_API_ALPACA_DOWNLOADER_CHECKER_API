"""Read-only lineage research and visibility; production publication is separate."""

VERSION = 3
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
    accepted_versions = {VERSION}
    bindings = [binding]
    if allow_previous:
        accepted_versions.add(2)
        if (binding["policy_lab"], binding["correlation_runtime"], binding["lineage_runtime"]) == (22, 95, 98):
            bindings.extend(
                {
                    "debug": version,
                    "policy_lab": 21,
                    "correlation_runtime": 95,
                    "membership_refinement": binding["membership_refinement"],
                }
                for version in (2, 3)
            )
    if (
        not isinstance(value, dict)
        or value.get("schema") != "alpaca.lineage.debug.report"
        or type(value.get("schema_version")) is not int
        or value["schema_version"] != SCHEMA_VERSION
        or type(value.get("debug_version")) is not int
        or value["debug_version"] not in accepted_versions
        or value.get("operation") != operation
        or value.get("implementation_binding") not in bindings
    ):
        raise ValueError("saved diagnostic report has an incompatible contract or research implementation")
