import unittest
from pathlib import Path

import yaml

BLUEPRINT_PATH = Path(__file__).parents[1] / "governance/catalog/governance-blueprint.yaml"


class GovernanceBlueprintTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.blueprint = yaml.safe_load(BLUEPRINT_PATH.read_text())

    def test_names_are_unique_and_references_resolve(self):
        domains = {item["name"] for item in self.blueprint["domains"]}
        products = [item["name"] for item in self.blueprint["data_products"]]
        self.assertEqual(len(products), len(set(products)))
        self.assertTrue(
            all(item["domain"] in domains for item in self.blueprint["data_products"])
        )

        classifications = self.blueprint["classifications"]
        classification_names = [item["name"] for item in classifications]
        self.assertEqual(len(classification_names), len(set(classification_names)))

        defined_tags = {
            f"{classification['name']}.{tag['name']}"
            for classification in classifications
            for tag in classification["tags"]
        }
        referenced_tags = {
            tag
            for mapping in self.blueprint["asset_tags"]
            for tag in mapping["tags"]
        }
        self.assertEqual(referenced_tags - defined_tags, set())

    def test_source_schema_tags_cover_all_synthetic_tables(self):
        expected_tables = {
            "mms": {"customers", "customer_addresses", "customer_contacts", "customer_relations"},
            "krd": {"loan_applications", "loans", "installments", "collaterals"},
            "prm": {"currencies", "branches", "products", "status_codes", "rate_parameters"},
        }
        mappings = self.blueprint["asset_tags"]

        for schema, tables in expected_tables.items():
            for table in tables:
                entity = f"synthetic_core_banking.core_banking.{schema}.{table}"
                mapping = next((item for item in mappings if item["entity"] == entity), None)
                with self.subTest(entity=entity):
                    self.assertIsNotNone(mapping)
                    self.assertIn(f"SourceDomain.{schema.upper()}", mapping["tags"])

    def test_asset_mappings_are_unique(self):
        entities = [item["entity"] for item in self.blueprint["asset_tags"]]
        self.assertEqual(len(entities), len(set(entities)))


if __name__ == "__main__":
    unittest.main()
