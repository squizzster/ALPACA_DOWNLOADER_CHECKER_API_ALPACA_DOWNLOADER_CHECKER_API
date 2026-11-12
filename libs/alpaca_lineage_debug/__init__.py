"""Read-only lineage research and visibility; production publication is separate."""

VERSION = 2
SCHEMA_VERSION = 1


def implementation_binding():
    from libs.alpaca_end_of_day_security_correlation.correlation_version import RUNTIME_VERSION
    from libs.alpaca_lineage_policy_lab.contract import LAB_VERSION

    return {
        "debug": VERSION,
        "policy_lab": LAB_VERSION,
        "correlation_runtime": RUNTIME_VERSION,
        "membership_refinement": "accepted_same_security_containing_component_v1",
    }


def accept_report(value, operation):
    if (
        not isinstance(value, dict)
        or value.get("schema") != "alpaca.lineage.debug.report"
        or type(value.get("schema_version")) is not int
        or value["schema_version"] != SCHEMA_VERSION
        or type(value.get("debug_version")) is not int
        or value["debug_version"] != VERSION
        or value.get("operation") != operation
        or value.get("implementation_binding") != implementation_binding()
    ):
        raise ValueError("saved diagnostic report has an incompatible contract or research implementation")
