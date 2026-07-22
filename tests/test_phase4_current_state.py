from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read(path: str) -> str:
    return (ROOT / path).read_text()


def test_raw_vault_has_hub_attached_status_and_effectivity_satellites():
    spark = read("scripts/4_process_cdc_raw_vault.py")

    for function_name in (
        "build_entity_record_status",
        "build_application_context_effectivity",
        "build_loan_context_effectivity",
        "bounded_entity_record_status",
        "bounded_application_context_effectivity",
        "bounded_loan_context_effectivity",
    ):
        assert f"def {function_name}" in spark
    for table_name in (
        "sat_entity_record_status",
        "sat_application_context_effectivity",
        "sat_loan_context_effectivity",
    ):
        assert table_name in spark
    assert 'f"$.{image}.{field_name}"' in spark
    assert 'choices=(' in spark and '"current-state-backfill"' in spark


def test_current_gold_models_share_one_active_entity_status_rule():
    models = (
        "dim_customer_current.sql",
        "dim_product_current.sql",
        "dim_branch_current.sql",
        "dim_currency_current.sql",
        "fct_loan_applications_current.sql",
        "fct_loans_current.sql",
    )
    for model_name in models:
        model = read(f"dbt/models/gold/{model_name}")
        assert "ref('int_current_entity_status')" in model
        assert "source_load_datetime" in model

    status = read("dbt/models/intermediate/int_current_entity_status.sql")
    assert "record_status = 'ACTIVE'" in status
    assert "not is_deleted" in status


def test_facts_use_effectivity_instead_of_historical_link_scan():
    application = read("dbt/models/gold/fct_loan_applications_current.sql")
    loan = read("dbt/models/gold/fct_loans_current.sql")

    assert "ref('int_current_application_context')" in application
    assert "source('cdc_raw_vault', 'link_application_context')" not in application
    assert "ref('int_current_loan_context')" in loan
    assert "source('cdc_raw_vault', 'link_loan_context')" not in loan
    assert "application_context_hk" in application
    assert "loan_context_hk" in loan


def test_delete_and_current_context_invariants_are_executable_dbt_tests():
    delete_test = read(
        "dbt/tests/assert_deleted_entities_absent_from_current_gold.sql"
    )
    context_test = read("dbt/tests/assert_gold_uses_current_effective_context.sql")

    assert "record_status = 'DELETED'" in delete_test
    assert "dim_customer_current" in delete_test
    assert "fct_loans_current" in delete_test
    assert "int_current_application_context" in context_test
    assert "int_current_loan_context" in context_test


def test_source_fixture_covers_delete_recreate_and_a_to_b_to_a_relationship():
    workload = read("source/workload/workload.py")
    migration = read("source/init/007_allow_current_state_workload.sql")

    for fixture_step in (
        '"setup"',
        '"delete"',
        '"recreate"',
        '"relation-b"',
        '"relation-a"',
    ):
        assert fixture_step in workload
    assert "PHASE4_LIFECYCLE_CUSTOMER" in workload
    assert "PHASE4_APPLICATION" in workload
    assert 'target_branch = "ANK001"' in workload
    assert "'current-state'" in migration


def test_new_delta_tables_are_registered_and_fingerprinted():
    registration = read("trino/register_tables.py")
    collector = read("operations/collect_phase0_baseline.py")
    source_contract = read("dbt/models/silver/_sources.yml")

    for table_name in (
        "sat_entity_record_status",
        "sat_application_context_effectivity",
        "sat_loan_context_effectivity",
    ):
        assert table_name in registration
        assert table_name in collector
        assert f"name: {table_name}" in source_contract


if __name__ == "__main__":
    tests = [
        value
        for name, value in sorted(globals().items())
        if name.startswith("test_") and callable(value)
    ]
    for test in tests:
        test()
        print(f"PASS {test.__name__}")
    print(f"PASS {len(tests)} phase 4 current-state tests")
