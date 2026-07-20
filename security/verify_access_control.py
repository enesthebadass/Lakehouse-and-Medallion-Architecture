"""Exercise the local Trino authentication and access-control proof of concept."""

from __future__ import annotations

import base64
import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any

TRINO_URL = os.environ.get("TRINO_URL", "http://localhost:8082").rstrip("/")


class TrinoError(RuntimeError):
    pass


@dataclass(frozen=True)
class Identity:
    username: str
    password: str

    @property
    def headers(self) -> dict[str, str]:
        token = base64.b64encode(f"{self.username}:{self.password}".encode()).decode()
        return {
            "Authorization": f"Basic {token}",
            "Content-Type": "text/plain; charset=utf-8",
            "X-Forwarded-Proto": "https",
            "X-Trino-User": self.username,
        }


def execute(identity: Identity, sql: str) -> list[list[Any]]:
    request = urllib.request.Request(
        f"{TRINO_URL}/v1/statement",
        data=sql.encode(),
        headers=identity.headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            page = json.load(response)
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        raise TrinoError(f"HTTP {exc.code}: {detail}") from exc

    rows: list[list[Any]] = []
    while True:
        if page.get("error"):
            raise TrinoError(page["error"].get("message", str(page["error"])))
        rows.extend(page.get("data", []))
        next_uri = page.get("nextUri")
        if not next_uri:
            return rows
        if TRINO_URL.startswith("http://") and next_uri.startswith("https://"):
            next_uri = "http://" + next_uri.removeprefix("https://")
        request = urllib.request.Request(next_uri, headers=identity.headers)
        with urllib.request.urlopen(request, timeout=30) as response:
            page = json.load(response)


def expect_denied(identity: Identity, sql: str) -> None:
    try:
        execute(identity, sql)
    except TrinoError as exc:
        if "denied" in str(exc).lower() or "cannot select" in str(exc).lower():
            return
        raise
    raise AssertionError(f"Expected access denial for {identity.username}: {sql}")


def main() -> None:
    admin = Identity("lakehouse-admin", os.getenv("TRINO_ADMIN_PASSWORD", "admin-local"))
    analyst = Identity("analyst", os.getenv("TRINO_ANALYST_PASSWORD", "analyst-local"))
    branch = Identity("branch-analyst", os.getenv("TRINO_BRANCH_PASSWORD", "branch-local"))
    auditor = Identity("auditor", os.getenv("TRINO_AUDITOR_PASSWORD", "auditor-local"))

    execute(admin, "SELECT count(*) FROM lakehouse.gold_dbt.dim_customer_current")
    execute(analyst, "SELECT count(*) FROM lakehouse.gold_dbt.dim_customer_current")
    expect_denied(analyst, "SELECT count(*) FROM lakehouse.cdc_raw_vault.hub_customer")

    admin_value = execute(
        admin,
        "SELECT national_id FROM lakehouse.cdc_raw_vault.sat_customer_details "
        "WHERE national_id IS NOT NULL ORDER BY customer_hk LIMIT 1",
    )[0][0]
    auditor_value = execute(
        auditor,
        "SELECT national_id FROM lakehouse.cdc_raw_vault.sat_customer_details "
        "WHERE national_id IS NOT NULL ORDER BY customer_hk LIMIT 1",
    )[0][0]
    assert auditor_value != admin_value
    assert auditor_value.endswith(admin_value[-4:])
    assert set(auditor_value[:-4]) == {"*"}

    unfiltered = execute(
        admin,
        "SELECT count(*) FROM lakehouse.gold_dbt.fct_loans_current "
        "WHERE branch_code <> 'BR001'",
    )[0][0]
    filtered = execute(
        branch,
        "SELECT count(*) FROM lakehouse.gold_dbt.fct_loans_current "
        "WHERE branch_code <> 'BR001'",
    )[0][0]
    assert unfiltered > 0
    assert filtered == 0

    try:
        execute(Identity("analyst", "wrong-password"), "SELECT 1")
    except TrinoError as exc:
        assert "401" in str(exc)
    else:
        raise AssertionError("Invalid password unexpectedly authenticated")

    print("Access-control PoC passed: auth, deny, mask, row filter")


if __name__ == "__main__":
    main()
