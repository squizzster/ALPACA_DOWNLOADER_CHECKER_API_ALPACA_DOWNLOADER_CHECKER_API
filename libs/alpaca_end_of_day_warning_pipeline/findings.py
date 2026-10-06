from collections import Counter
from dataclasses import dataclass, field
from typing import Any

from .settings import WarningPipelineConfiguration
from .warning_pipeline_version import WARNING_PIPELINE_VERSION

WARNING = "warning"
INFORMATION = "information"


@dataclass
class FindingCollector:
    configuration: WarningPipelineConfiguration
    findings: list[dict[str, Any]] = field(default_factory=list)
    evaluation_counts: Counter = field(default_factory=Counter)
    violation_counts: Counter = field(default_factory=Counter)
    severity_counts: Counter = field(default_factory=Counter)
    check_severity_counts: Counter = field(default_factory=Counter)
    omitted_finding_count: int = 0

    def evaluated(self, check_id: str, count: int = 1) -> None:
        self.evaluation_counts[check_id] += count

    def add(
        self,
        check_id: str,
        severity: str,
        message: str,
        *,
        data_files: list[str] | tuple[str, ...] = (),
        evidence: dict[str, Any] | None = None,
        confidence: str,
        basis: str,
        canonical_security: dict[str, Any] | None = None,
    ) -> None:
        if severity not in {WARNING, INFORMATION}:
            raise ValueError(f"unsupported market-data observation severity: {severity!r}")
        self.violation_counts[check_id] += 1
        self.severity_counts[severity] += 1
        self.check_severity_counts[(check_id, severity)] += 1
        if len(self.findings) >= self.configuration.maximum_findings:
            self.omitted_finding_count += 1
            return
        finding = {
            "warning_pipeline_version": WARNING_PIPELINE_VERSION,
            "check_id": check_id,
            "severity": severity,
            "confidence": confidence,
            "message": message,
            "basis": basis,
            "data_files": list(data_files),
            "evidence": evidence or {},
        }
        if canonical_security is not None:
            finding["canonical_security"] = canonical_security
        self.findings.append(finding)

    def as_report_object(self) -> dict[str, Any]:
        check_ids = sorted(set(self.evaluation_counts) | set(self.violation_counts))
        checks = [
            {
                "check_id": check_id,
                "evaluated": self.evaluation_counts[check_id],
                "warning_count": self.check_severity_counts[(check_id, WARNING)],
                "information_count": self.check_severity_counts[(check_id, INFORMATION)],
                "total_finding_count": self.violation_counts[check_id],
            }
            for check_id in check_ids
        ]
        return {
            "warning_count": self.severity_counts[WARNING],
            "information_count": self.severity_counts[INFORMATION],
            "total_finding_count": sum(self.violation_counts.values()),
            "omitted_finding_count": self.omitted_finding_count,
            "checks": checks,
            "findings": self.findings,
        }
