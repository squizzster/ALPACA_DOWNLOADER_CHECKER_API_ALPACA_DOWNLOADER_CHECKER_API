"""Compose new public-holder proof with dated metadata without recertifying peers."""

from libs.alpaca_end_of_day_security_correlation import public_common_continuation
from libs.alpaca_security_lineage.sourced_facts import EXTENSIONS, PUBLIC_COMMON_KIND, items

from .contract import LineageError


def _assertions(evidences):
    selected = {}
    for value in evidences:
        for fact in items(value, PUBLIC_COMMON_KIND):
            identifier = fact["match"]["source_id"]
            if identifier in selected and selected[identifier] != fact:
                raise LineageError("conflicting public continuation assertions require owning reconciliation")
            selected[identifier] = fact
    return selected


def bind(controls, evidence, cache):
    from .source_work import compose_memberships

    key = tuple((name, id(value)) for name, value in sorted(evidence.items()))
    completed = cache.setdefault("documented_continuities", {})
    if key in completed:
        return completed[key]
    old_facts = _assertions(control.previous["evidence"] for control in controls.values())
    submitted = _assertions(evidence.values())
    if any(submitted.get(identifier) != fact for identifier, fact in old_facts.items()):
        raise LineageError("changed accepted public continuation requires owning reconciliation")
    certificates, issues = {}, []
    for control in controls.values():
        prior = control.previous.get("public_common_continuation_membership") or {}
        for certificate in prior.get("certificates", ()):
            identifier = certificate["source_fact_id"]
            if identifier in certificates and certificates[identifier] != certificate:
                raise LineageError("conflicting accepted public continuation requires owning reconciliation")
            certificates[identifier] = certificate
        issues.extend(prior.get("issues", ()))
    fresh = submitted.keys() - old_facts.keys()
    if fresh:
        extension = EXTENSIONS[PUBLIC_COMMON_KIND]
        new_evidence = [
            {
                **value,
                extension: {
                    "schema_version": 1,
                    "facts": [
                        fact for fact in items(value, PUBLIC_COMMON_KIND) if fact["match"]["source_id"] in fresh
                    ],
                },
            }
            for value in evidence.values()
        ]
        selected = public_common_continuation.certify_group(new_evidence)
        if selected["issues"] or {item["source_fact_id"] for item in selected["certificates"]} != fresh:
            raise LineageError("new documented public continuation requires owning endpoint reconciliation")
        for certificate in selected["certificates"]:
            endpoints = {certificate[role]["asset_id"] for role in ("predecessor", "successor")}
            if any(
                endpoints & {old[role]["asset_id"] for role in ("predecessor", "successor")}
                for old in certificates.values()
            ):
                raise LineageError("overlapping public continuation requires owning branch reconciliation")
            certificates[certificate["source_fact_id"]] = certificate
    common = {
        "version": 1,
        "certificates": [certificates[identifier] for identifier in sorted(certificates)],
        "issues": [item for index, item in enumerate(issues) if item not in issues[:index]],
    }
    if not fresh:
        completed[key] = common, None
        return completed[key]
    prior_members = {
        identifier: identity
        for control in controls.values()
        for identity in control.previous["identity_memberships"]
        for identifier in identity["member_alpaca_asset_ids"]
    }
    members = compose_memberships(
        prior_members,
        [
            public_common_continuation.CommonCertificate(**certificates[identifier]).identity_edge()
            for identifier in sorted(fresh)
        ],
    )
    completed[key] = common, members
    return completed[key]
