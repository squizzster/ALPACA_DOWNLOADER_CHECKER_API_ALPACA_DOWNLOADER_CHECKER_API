"""Read-only lineage research and visibility; production publication is separate."""

VERSION = 28
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
    reject_fact_report(value)
    binding = implementation_binding()
    bindings = [binding]
    current = tuple(binding[name] for name in ("debug", "policy_lab", "correlation_runtime", "lineage_runtime"))
    if allow_previous and current == (28, 27, 114, 133):
        # Frozen comparisons retain their own split and rename policy.
        binding = {**binding, "correlation_runtime": 113, "lineage_runtime": 132}
        bindings.append(binding)
        current = (28, 27, 113, 132)
    if allow_previous and current == (28, 27, 113, 132):
        # Frozen comparisons retain the policy that produced their selected output.
        binding = {**binding, "correlation_runtime": 112, "lineage_runtime": 131}
        bindings.append(binding)
        current = (28, 27, 112, 131)
    if allow_previous and current == (28, 27, 112, 131):
        # The new bridge has a separate policy identity. Saved comparisons bind that identity independently.
        binding = {**binding, "correlation_runtime": 111, "lineage_runtime": 130}
        bindings.append(binding)
        current = (28, 27, 111, 130)
    if allow_previous and current == (28, 27, 111, 130):
        # Completed comparisons keep their original policy and group generations.
        binding = {**binding, "debug": 27, "correlation_runtime": 110, "lineage_runtime": 129}
        bindings.append(binding)
        current = (27, 27, 110, 129)
    if allow_previous and current == (27, 27, 110, 129):
        binding = {**binding, "correlation_runtime": 109, "lineage_runtime": 128}
        bindings.append(binding)
        current = (27, 27, 109, 128)
    if allow_previous and current == (27, 27, 109, 128):
        binding = {**binding, "lineage_runtime": 127}
        bindings.append(binding)
        current = (27, 27, 109, 127)
    if allow_previous and current == (27, 27, 109, 127):
        binding = {**binding, "correlation_runtime": 108, "lineage_runtime": 126}
        bindings.append(binding)
        current = (27, 27, 108, 126)
    if allow_previous and current == (27, 27, 108, 126):
        binding = {**binding, "lineage_runtime": 125}
        bindings.append(binding)
        current = (27, 27, 108, 125)
    if allow_previous and current == (27, 27, 108, 125):
        binding = {**binding, "correlation_runtime": 107}
        bindings.append(binding)
        current = (27, 27, 107, 125)
    if allow_previous and current == (27, 27, 107, 125):
        binding = {**binding, "correlation_runtime": 106}
        bindings.append(binding)
        current = (27, 27, 106, 125)
    if allow_previous and current == (27, 27, 106, 125):
        binding = {**binding, "correlation_runtime": 105}
        bindings.append(binding)
        current = (27, 27, 105, 125)
    if allow_previous and current == (27, 27, 105, 125):
        binding = {**binding, "correlation_runtime": 104}
        bindings.append(binding)
        current = (27, 27, 104, 125)
    if allow_previous and current == (27, 27, 104, 125):
        binding = {**binding, "correlation_runtime": 103}
        bindings.append(binding)
        current = (27, 27, 103, 125)
    if allow_previous and current == (27, 27, 103, 125):
        binding = {**binding, "correlation_runtime": 102}
        bindings.append(binding)
        current = (27, 27, 102, 125)
    if allow_previous and current == (27, 27, 102, 125):
        # Frozen comparisons retain the recorded policy and generation bindings.
        binding = {**binding, "correlation_runtime": 101, "lineage_runtime": 124}
        bindings.append(binding)
        current = (27, 27, 101, 124)
    if allow_previous and current == (27, 27, 101, 124):
        # Distribution ownership has a distinct policy identity. Frozen
        # comparisons retain their exact policy and generation bindings.
        binding = {**binding, "correlation_runtime": 100}
        bindings.append(binding)
        current = (27, 27, 100, 124)
    if allow_previous and current == (27, 27, 100, 124):
        # Workflow catalogue retention leaves the recorded policy, generations
        # and completed lineage comparisons applicable under their original bindings.
        binding = {**binding, "debug": 26, "lineage_runtime": 123}
        bindings.append(binding)
        current = (26, 27, 100, 123)
    if allow_previous and current == (26, 27, 100, 123):
        # Pricing insertions and the public range change neither recorded
        # lineage comparisons nor their input-generation and policy bindings.
        binding = {**binding, "debug": 25, "lineage_runtime": 122}
        bindings.append(binding)
        current = (25, 27, 100, 122)
    if allow_previous and current == (25, 27, 100, 122):
        # Update scheduling and serving changed. Selected lineage comparisons
        # still have the same policy, evidence and input-generation contracts.
        binding = {**binding, "debug": 24, "lineage_runtime": 121}
        bindings.append(binding)
        current = (24, 27, 100, 121)
    if allow_previous and current == (24, 27, 100, 121):
        # Frozen reports keep their original policy bindings. The caller
        # checks both policies and the selected generations before reuse.
        binding = {**binding, "debug": 23, "policy_lab": 26, "correlation_runtime": 99, "lineage_runtime": 120}
        bindings.append(binding)
        current = (23, 26, 99, 120)
    if allow_previous and current == (23, 26, 99, 120):
        # Workflow retry scope does not change completed diagnostic calculations.
        bindings.append({**binding, "debug": 22, "lineage_runtime": 119})
        current = (22, 26, 99, 119)
    if allow_previous and current == (22, 26, 99, 119):
        # Witness carry changes publication only. Completed comparisons keep
        # their original diagnostic and policy bindings.
        bindings.append({**binding, "debug": 21, "lineage_runtime": 118})
        current = (21, 26, 99, 118)
    if allow_previous and current == (21, 26, 99, 118):
        bindings.append({**binding, "debug": 20, "lineage_runtime": 117})
        current = (20, 26, 99, 117)
    if allow_previous and current == (20, 26, 99, 117):
        # Only production publication changed. The diagnostic producer can use
        # exactly the reports its preceding release accepted, with their own bindings.
        bindings.append({**binding, "debug": 19, "lineage_runtime": 116})
        current = (19, 26, 99, 116)
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
    if allow_previous and current in {
        (9, 23, 97, 104),
        (10, 23, 97, 105),
        (11, 24, 97, 106),
        (12, 24, 97, 107),
        (13, 24, 97, 108),
        (13, 24, 97, 109),
        (13, 24, 97, 110),
        (14, 24, 97, 111),
        (15, 24, 97, 112),
        (16, 25, 98, 113),
        (17, 26, 99, 114),
        (18, 26, 99, 115),
        (19, 26, 99, 116),
    }:
        # Generic completed comparisons retain their exact policy/generation
        # bindings. Runtimes 109 through 111 change production mapping acquisition and
        # owning recovery without changing these research producers. Factual candidates are rejected
        # above, never restored.
        bindings.extend(
            {
                "debug": debug,
                "policy_lab": lab,
                "correlation_runtime": correlation,
                **({"lineage_runtime": lineage} if lineage is not None else {}),
                "membership_refinement": binding["membership_refinement"],
            }
            for debug, lab, correlation, lineage in (
                (2, 21, 95, None),
                (3, 21, 95, None),
                (3, 22, 95, 98),
                (4, 22, 96, 99),
                (5, 22, 96, 100),
                (6, 22, 96, 100),
                (6, 23, 96, 100),
                (6, 23, 97, 101),
                (6, 23, 97, 102),
                (7, 23, 97, 102),
                (8, 23, 97, 103),
                (9, 23, 97, 104),
                (10, 23, 97, 105),
                (11, 24, 97, 106),
                (12, 24, 97, 107),
                (13, 24, 97, 108),
                (13, 24, 97, 109),
                (13, 24, 97, 110),
                (14, 24, 97, 111),
            )
        )
        if current in {(16, 25, 98, 113), (17, 26, 99, 114), (18, 26, 99, 115), (19, 26, 99, 116)}:
            # Explicit older-policy replay retains the numerical selection
            # semantics recorded in these completed comparisons.
            bindings.append(
                {**binding, "debug": 15, "policy_lab": 24, "correlation_runtime": 97, "lineage_runtime": 112}
            )
        if current in {(17, 26, 99, 114), (18, 26, 99, 115), (19, 26, 99, 116)}:
            bindings.append(
                {**binding, "debug": 16, "policy_lab": 25, "correlation_runtime": 98, "lineage_runtime": 113}
            )
        if current in {(18, 26, 99, 115), (19, 26, 99, 116)}:
            bindings.append({**binding, "debug": 17, "lineage_runtime": 114})
        if current == (19, 26, 99, 116):
            bindings.append({**binding, "debug": 18, "lineage_runtime": 115})
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


def reject_fact_report(value):
    """Completed generic results can be lent; factual candidates cannot."""
    _reject_fact_options(value)


def _reject_fact_options(value):
    if isinstance(value, dict):
        from libs.alpaca_security_lineage.inference_evidence import ASSERTION_FIELDS, reject_assertions

        reject_assertions(
            {name: item for name, item in value.items() if name in ASSERTION_FIELDS},
            where="saved diagnostic candidate",
        )
        if "documented_facts" in value or any(
            value.get(name) for name in ("rule_applications", "documented_plans", "rule_fact_scopes")
        ):
            raise ValueError("saved diagnostic report contains fact-backed research inputs")
        if value.get("control_mode", "replay") != "replay":
            raise ValueError("saved diagnostic report contains a fact-backed control mode")
        for name, item in value.items():
            # Frozen publication bodies are comparison data, never fresh
            # candidate evidence. Their legacy provenance remains inspectable.
            if name not in {"receipts", "boundaries", "reports"}:
                _reject_fact_options(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _reject_fact_options(item)
