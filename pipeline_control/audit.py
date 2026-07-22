"""Build and validate immutable point-in-time reconciliation evidence."""

from __future__ import annotations

import hashlib
import re
from typing import Any, Iterable, Mapping

from pipeline_control.manifest import read_immutable_json, write_immutable_json

AUDIT_SCHEMA_VERSION = 1
AUDIT_RULE_VERSION = "raw-vault-audit-v1"


class AuditEvidenceError(RuntimeError):
    """Raised when reconciliation evidence is incomplete or inconsistent."""


def audit_evidence_uri(bucket: str, batch_id: str, attempt_number: int) -> str:
    readable = re.sub(r"[^A-Za-z0-9._-]+", "-", batch_id).strip("-.")[:80] or "batch"
    digest = hashlib.sha256(batch_id.encode("utf-8")).hexdigest()[:16]
    return (
        f"s3://{bucket}/bronze/_control/audit/{readable}-{digest}/"
        f"attempt={attempt_number}.json"
    )


def build_audit_payload(
    *,
    batch_id: str,
    attempt_number: int,
    airflow_run_id: str,
    manifest_sha256: str,
    manifest: Mapping[str, Any],
    rules: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    normalized_rules = sorted(
        (dict(rule) for rule in rules),
        key=lambda rule: (str(rule["rule_id"]), str(rule["object_name"])),
    )
    return {
        "schema_version": AUDIT_SCHEMA_VERSION,
        "rule_version": AUDIT_RULE_VERSION,
        "batch_id": batch_id,
        "attempt_number": attempt_number,
        "airflow_run_id": airflow_run_id,
        "manifest_sha256": manifest_sha256,
        "watermark": [
            {
                "topic": bound["topic"],
                "partition": bound["partition"],
                "watermark_low": bound["watermark_low"],
                "watermark_high": bound["watermark_high"],
            }
            for bound in manifest["partitions"]
        ],
        "source_control": manifest.get("source_control"),
        "rules": normalized_rules,
    }


def validate_audit_payload(
    payload: Mapping[str, Any],
    *,
    expected_batch_id: str,
    expected_attempt_number: int,
    expected_airflow_run_id: str,
    expected_manifest_sha256: str,
) -> dict[str, Any]:
    expected = {
        "schema_version": AUDIT_SCHEMA_VERSION,
        "rule_version": AUDIT_RULE_VERSION,
        "batch_id": expected_batch_id,
        "attempt_number": expected_attempt_number,
        "airflow_run_id": expected_airflow_run_id,
        "manifest_sha256": expected_manifest_sha256,
    }
    for key, value in expected.items():
        if payload.get(key) != value:
            raise AuditEvidenceError(f"Audit evidence {key} does not match its attempt")
    watermark = payload.get("watermark")
    if not isinstance(watermark, list) or not watermark:
        raise AuditEvidenceError("Audit evidence requires a non-empty watermark")
    rules = payload.get("rules")
    if not isinstance(rules, list) or not rules:
        raise AuditEvidenceError("Audit evidence requires at least one rule")
    identities = []
    for rule in rules:
        if not isinstance(rule, dict):
            raise AuditEvidenceError("Audit rule must be an object")
        identity = (rule.get("rule_id"), rule.get("object_name"))
        identities.append(identity)
        if not all(isinstance(item, str) and item for item in identity):
            raise AuditEvidenceError("Audit rule identity must be non-empty")
        if rule.get("status") not in {"PASS", "FAIL"}:
            raise AuditEvidenceError("Audit rule status must be PASS or FAIL")
        for field in ("expected_value", "observed_value", "difference"):
            if not isinstance(rule.get(field), int):
                raise AuditEvidenceError(f"Audit rule {field} must be an integer")
        if rule["difference"] != rule["observed_value"] - rule["expected_value"]:
            raise AuditEvidenceError("Audit rule difference is inconsistent")
        expected_status = (
            "PASS" if rule["expected_value"] == rule["observed_value"] else "FAIL"
        )
        if rule["status"] != expected_status:
            raise AuditEvidenceError("Audit rule status is inconsistent with its values")
    if identities != sorted(set(identities)):
        raise AuditEvidenceError("Audit rules must be unique and sorted")
    return dict(payload)


def write_audit_evidence(client: Any, uri: str, payload: Mapping[str, Any]) -> str:
    return write_immutable_json(client, uri, payload)


def read_audit_evidence(
    client: Any,
    uri: str,
    **expected: Any,
) -> tuple[dict[str, Any], str]:
    payload, checksum = read_immutable_json(client, uri)
    return validate_audit_payload(payload, **expected), checksum
