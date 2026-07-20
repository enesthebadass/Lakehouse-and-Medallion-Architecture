"""Consistency and security checks for the target platform blueprint."""

from __future__ import annotations

import re
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
PLATFORM = ROOT / "platform"
BLUEPRINT = yaml.safe_load((PLATFORM / "deployment-responsibilities.yaml").read_text())
IAM = yaml.safe_load((PLATFORM / "iam-group-mapping.yaml").read_text())
EXAMPLES = PLATFORM / "kubernetes/examples"


class PlatformBlueprintTests(unittest.TestCase):
    def test_all_target_components_have_owners_and_valid_namespaces(self) -> None:
        expected = {"airflow", "spark", "kafka", "trino", "openmetadata", "ranger"}
        components = {component["name"]: component for component in BLUEPRINT["components"]}
        namespaces = {namespace["name"] for namespace in BLUEPRINT["namespaces"]}
        self.assertEqual(set(components), expected)
        for component in components.values():
            self.assertIn(component["namespace"], namespaces)
            self.assertTrue(component["platform_owner"])
            self.assertTrue(component["application_owner"])
            self.assertTrue(component["production_requirements"])

    def test_runtime_status_does_not_overclaim_cluster_implementation(self) -> None:
        status = BLUEPRINT["status"]
        self.assertEqual(status["opa_offline_policy"], "implemented_poc")
        self.assertEqual(status["kubernetes"], "designed_external")
        self.assertEqual(status["openshift"], "designed_external")
        self.assertEqual(status["gatekeeper"], "designed_external")

    def test_iam_uses_groups_and_non_interactive_service_accounts(self) -> None:
        self.assertEqual(IAM["rules"]["direct_user_role_binding"], "forbidden")
        self.assertEqual(IAM["rules"]["default_service_account"], "forbidden")
        groups = [mapping["enterprise_group"] for mapping in IAM["group_mappings"]]
        self.assertEqual(len(groups), len(set(groups)))
        for account in IAM["service_accounts"]:
            self.assertEqual(account["interactive_login"], "forbidden")
            self.assertEqual(account["workload_identity"], "required")

    def test_example_workload_meets_security_baseline(self) -> None:
        workload = yaml.safe_load((EXAMPLES / "trino-deployment.yaml").read_text())
        pod = workload["spec"]["template"]["spec"]
        self.assertNotEqual(pod["serviceAccountName"], "default")
        self.assertFalse(pod["automountServiceAccountToken"])
        self.assertTrue(pod["securityContext"]["runAsNonRoot"])
        self.assertEqual(pod["securityContext"]["seccompProfile"]["type"], "RuntimeDefault")
        for container in pod["containers"]:
            self.assertRegex(container["image"], r"^registry\.bank\.internal/.+@sha256:[0-9a-f]{64}$")
            self.assertEqual(set(container["resources"]), {"requests", "limits"})
            self.assertFalse(container["securityContext"]["allowPrivilegeEscalation"])
            self.assertTrue(container["securityContext"]["readOnlyRootFilesystem"])
            self.assertIn("ALL", container["securityContext"]["capabilities"]["drop"])

    def test_sensitive_environment_values_use_secret_references(self) -> None:
        workload = yaml.safe_load((EXAMPLES / "trino-deployment.yaml").read_text())
        containers = workload["spec"]["template"]["spec"]["containers"]
        sensitive = re.compile(r"password|token|secret|key", re.IGNORECASE)
        for container in containers:
            for env in container.get("env", []):
                if sensitive.search(env["name"]):
                    self.assertNotIn("value", env)
                    self.assertIn("secretKeyRef", env["valueFrom"])

    def test_namespace_and_network_policy_are_restricted_by_default(self) -> None:
        namespace = yaml.safe_load((EXAMPLES / "namespace.yaml").read_text())
        labels = namespace["metadata"]["labels"]
        self.assertEqual(labels["pod-security.kubernetes.io/enforce"], "restricted")
        policy = yaml.safe_load((EXAMPLES / "default-deny-network-policy.yaml").read_text())
        self.assertEqual(policy["spec"]["podSelector"], {})
        self.assertEqual(set(policy["spec"]["policyTypes"]), {"Ingress", "Egress"})


if __name__ == "__main__":
    unittest.main()
