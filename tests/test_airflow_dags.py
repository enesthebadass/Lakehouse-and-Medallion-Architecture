import os
import unittest

from airflow.models import DagBag

EXPECTED_TASKS = {
    "cdc_raw_vault_incremental": {
        "create_batch_manifest",
        "start_batch_attempt",
        "validate_bronze_and_quarantine",
        "record_validate_bronze_and_quarantine_evidence",
        "load_incremental_hubs_links",
        "record_load_incremental_hubs_links_evidence",
        "load_satellite_and_delete_history",
        "record_load_satellite_and_delete_history_evidence",
        "reconcile_source_bronze_silver",
        "record_reconcile_source_bronze_silver_evidence",
        "publish_raw_vault_batch",
    },
    "lakehouse_medallion_pipeline": {
        "generate_bronze_dirty_data",
        "process_silver_data_vault",
        "process_gold_star_schema",
    },
}


class AirflowDagTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dag_bag = DagBag(
            dag_folder=os.environ.get("AIRFLOW_DAG_FOLDER", "dags"),
            include_examples=False,
        )

    def test_dags_import_without_errors(self):
        self.assertEqual(self.dag_bag.import_errors, {})

    def test_expected_dags_and_tasks_exist(self):
        for dag_id, expected_tasks in EXPECTED_TASKS.items():
            with self.subTest(dag_id=dag_id):
                dag = self.dag_bag.dags.get(dag_id)
                self.assertIsNotNone(dag)
                self.assertEqual(set(dag.task_ids), expected_tasks)

    def test_cdc_raw_vault_dependency_chain(self):
        dag = self.dag_bag.dags["cdc_raw_vault_incremental"]
        expected_edges = {
            ("create_batch_manifest", "start_batch_attempt"),
            ("start_batch_attempt", "validate_bronze_and_quarantine"),
            (
                "validate_bronze_and_quarantine",
                "record_validate_bronze_and_quarantine_evidence",
            ),
            (
                "record_validate_bronze_and_quarantine_evidence",
                "load_incremental_hubs_links",
            ),
            (
                "load_incremental_hubs_links",
                "record_load_incremental_hubs_links_evidence",
            ),
            (
                "record_load_incremental_hubs_links_evidence",
                "load_satellite_and_delete_history",
            ),
            (
                "load_satellite_and_delete_history",
                "record_load_satellite_and_delete_history_evidence",
            ),
            (
                "record_load_satellite_and_delete_history_evidence",
                "reconcile_source_bronze_silver",
            ),
            (
                "reconcile_source_bronze_silver",
                "record_reconcile_source_bronze_silver_evidence",
            ),
            (
                "record_reconcile_source_bronze_silver_evidence",
                "publish_raw_vault_batch",
            ),
        }
        actual_edges = {
            (task.task_id, downstream_id)
            for task in dag.tasks
            for downstream_id in task.downstream_task_ids
        }
        self.assertEqual(actual_edges, expected_edges)

    def test_cdc_raw_vault_tasks_share_the_same_manifest_boundary(self):
        dag = self.dag_bag.dags["cdc_raw_vault_incremental"]
        spark_task_ids = (
            "validate_bronze_and_quarantine",
            "load_incremental_hubs_links",
            "load_satellite_and_delete_history",
            "reconcile_source_bronze_silver",
        )
        boundary_args = []

        for task_id in spark_task_ids:
            application_args = dag.get_task(task_id).application_args
            self.assertIn("--batch-id", application_args)
            self.assertIn("--manifest-uri", application_args)
            self.assertIn("--manifest-sha256", application_args)
            boundary_args.append(tuple(application_args[-6:]))

        self.assertEqual(len(set(boundary_args)), 1)


if __name__ == "__main__":
    unittest.main()
