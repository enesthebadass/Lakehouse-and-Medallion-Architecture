from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_reproducibility_script_is_isolated_and_guarded():
    script = (ROOT / "operations/reproducibility_test.sh").read_text()

    assert "^lakehouse-repro-" in script
    assert '== "lakehouse-medallion-demo"' in script
    assert "Fresh project volumes" in script
    assert "end_to_end_smoke_test.sh --exercise-change" in script
    assert "docker-compose.observability.yml" in script
    assert "down --volumes --remove-orphans" in script


def test_clean_start_snapshot_race_regression_is_guarded():
    writer = (ROOT / "cdc/bronze/writer.py").read_text()
    raw_vault = (ROOT / "scripts/4_process_cdc_raw_vault.py").read_text()
    smoke_test = (ROOT / "operations/end_to_end_smoke_test.sh").read_text()

    assert '"topic.metadata.refresh.interval.ms": 10000' in writer
    assert "def envelope_field" in raw_vault
    assert 'F.col("value.after.' not in raw_vault
    assert 'EXPECTED_CDC_TOPIC_COUNT="' + '$' + '{EXPECTED_CDC_TOPIC_COUNT:-13}"' in smoke_test
    assert 'CDC_LAG_STABLE_SAMPLES="' + '$' + '{CDC_LAG_STABLE_SAMPLES:-6}"' in smoke_test


def test_readme_uses_source_first_profiled_quick_start():
    readme = (ROOT / "README.md").read_text()

    source_start = "docker compose up -d --build core-banking-source"
    snapshot = "docker compose --profile tools run --rm core-banking-workload snapshot"
    remaining_stack = "docker compose up -d --build"
    assert readme.index(source_start) < readme.index(snapshot)
    assert readme.index(snapshot) < readme.index(remaining_stack, readme.index(snapshot))
    assert "core-banking-workload +" not in readme
    assert "snapshot +" not in readme
    assert "changes +" not in readme


if __name__ == "__main__":
    tests = [
        value
        for name, value in sorted(globals().items())
        if name.startswith("test_") and callable(value)
    ]
    for test in tests:
        test()
        print(f"PASS {test.__name__}")
    print(f"PASS {len(tests)} final package tests")
