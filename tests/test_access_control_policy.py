"""Static checks for the executable PoC and production Ranger policy package."""

from __future__ import annotations

import json
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
MATRIX = yaml.safe_load((ROOT / "security/access-matrix.yaml").read_text())
RANGER = yaml.safe_load((ROOT / "security/ranger/policy-spec.yaml").read_text())
RULES = json.loads((ROOT / "security/trino/rules.json").read_text())


class AccessControlPolicyTests(unittest.TestCase):
    def test_roles_have_unique_local_identities_and_all_layers(self) -> None:
        roles = MATRIX["roles"]
        self.assertEqual(len(roles), len({role["name"] for role in roles}))
        self.assertEqual(len(roles), len({role["local_user"] for role in roles}))
        for role in roles:
            self.assertTrue({"bronze", "silver", "gold", "audit"} <= role.keys())

    def test_local_policy_is_deny_by_default_and_covers_every_identity(self) -> None:
        self.assertEqual(MATRIX["principles"]["default_decision"], "deny")
        self.assertEqual(RULES["catalogs"][-1]["allow"], "none")
        self.assertEqual(RULES["tables"][-1]["privileges"], [])
        configured_users = " ".join(
            rule.get("user", "")
            for section in ("catalogs", "tables")
            for rule in RULES[section]
        )
        for role in MATRIX["roles"]:
            self.assertIn(role["local_user"], configured_users)

    def test_sensitive_classifications_have_runtime_actions(self) -> None:
        mappings = {
            item["classification"]: item["action"]
            for item in MATRIX["classification_policy_mapping"]
        }
        self.assertEqual(mappings["PersonalData.SensitiveIdentifier"], "mask")
        self.assertEqual(
            mappings["DataSensitivity.Restricted"],
            "deny_unless_explicitly_approved",
        )
        self.assertTrue(MATRIX["column_masks"])

    def test_ranger_package_preserves_masks_filters_and_audit(self) -> None:
        self.assertEqual(RANGER["status"], "designed_external")
        self.assertGreaterEqual(RANGER["minimum_ranger_version"], "2.5.0")
        self.assertEqual(len(RANGER["masking_policies"]), len(MATRIX["column_masks"]))
        self.assertEqual(len(RANGER["row_filter_policies"]), len(MATRIX["row_filters"]))
        self.assertTrue(RANGER["audit"]["capture_allowed"])
        self.assertTrue(RANGER["audit"]["capture_denied"])

    def test_dbt_ranger_policy_cannot_write_to_raw_vault(self) -> None:
        dbt_policies = {
            policy["name"]: policy for policy in RANGER["data_policies"]
        }
        self.assertEqual(dbt_policies["dbt-source-read"]["access"], ["select"])
        self.assertEqual(
            dbt_policies["dbt-source-read"]["resources"]["schema"],
            "cdc_raw_vault",
        )
        self.assertEqual(
            dbt_policies["dbt-gold-write"]["resources"]["schema"],
            "gold_dbt",
        )

    def test_password_file_has_only_declared_local_identities(self) -> None:
        users = {
            line.split(":", 1)[0]
            for line in (ROOT / "security/trino/password.db").read_text().splitlines()
            if line
        }
        self.assertEqual(users, {role["local_user"] for role in MATRIX["roles"]})

    def test_security_overlay_keeps_base_catalog_and_runtime_files_in_sync(self) -> None:
        relative_paths = (
            "catalog/lakehouse.properties",
            "jvm.config",
            "log.properties",
            "node.properties",
        )
        for relative_path in relative_paths:
            self.assertEqual(
                (ROOT / "security/trino" / relative_path).read_text(),
                (ROOT / "trino/etc" / relative_path).read_text(),
            )


if __name__ == "__main__":
    unittest.main()
