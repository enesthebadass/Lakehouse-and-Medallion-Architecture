import json
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
CONNECTOR_PATH = (
    ROOT / "cdc" / "connectors" / "core-banking-oracle-logminer.template.json"
)
READINESS_PATH = ROOT / "cdc" / "oracle-readiness.yaml"


def load_connector():
    return json.loads(CONNECTOR_PATH.read_text(encoding="utf-8"))


def load_readiness():
    return yaml.safe_load(READINESS_PATH.read_text(encoding="utf-8"))


def test_connector_selects_logminer_without_goldengate_components():
    connector = load_connector()

    assert connector["connector.class"] == (
        "io.debezium.connector.oracle.OracleConnector"
    )
    assert connector["database.connection.adapter"] == "logminer"
    assert "xstream" not in json.dumps(connector).lower()
    assert connector["schema.include.list"] == "MMS,KRD,PRM"


def test_connector_is_allowlisted_and_not_accidentally_deployable():
    connector = load_connector()

    assert connector["table.include.list"].startswith("__REPLACE_")
    assert connector["database.hostname"].startswith("__INJECT_")
    assert connector["database.user"].startswith("__INJECT_")
    assert connector["database.password"].startswith("__INJECT_")
    assert connector["errors.tolerance"] == "none"


def test_snapshot_and_event_contract_are_explicit():
    connector = load_connector()
    readiness = load_readiness()

    assert connector["snapshot.mode"] == "initial"
    assert connector["provide.transaction.metadata"] == "true"
    assert set(readiness["event_contract"]["canonical_operations"].values()) == {
        "r",
        "c",
        "u",
        "d",
    }
    assert len(readiness["snapshot_to_streaming"]["gates"]) >= 7


def test_external_owners_and_production_gates_are_complete():
    readiness = load_readiness()

    expected_owners = {
        "oracle_dba",
        "source_owner",
        "security",
        "network",
        "license_management",
    }
    assert expected_owners == set(readiness["external_dependencies"])
    assert len(readiness["database_prerequisites"]) >= 7
    assert len(readiness["production_gates"]) >= 7


def test_blueprint_does_not_overclaim_oracle_validation():
    readiness = load_readiness()

    assert readiness["status"] == "designed_external"
    assert readiness["scope"]["local_postgresql_pilot"] == "implemented_poc"
    assert readiness["scope"]["oracle_database_validation"] == (
        "blocked_by_external_access"
    )
    assert any(
        "does not validate Oracle" in statement
        for statement in readiness["non_claims"]
    )


if __name__ == "__main__":
    tests = [
        value
        for name, value in sorted(globals().items())
        if name.startswith("test_") and callable(value)
    ]
    for test in tests:
        test()
        print(f"PASS {test.__name__}")
    print(f"PASS {len(tests)} Oracle CDC blueprint tests")
