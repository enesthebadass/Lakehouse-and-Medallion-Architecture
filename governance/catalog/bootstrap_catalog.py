"""Apply the reviewed governance blueprint to OpenMetadata through its REST API."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen

import yaml

OPENMETADATA_HOST = os.environ.get("OPENMETADATA_HOST", "http://openmetadata-server:8585/api")
JWT_TOKEN = os.environ.get("OPENMETADATA_JWT_TOKEN", "")
BLUEPRINT_PATH = Path(
    os.environ.get(
        "GOVERNANCE_BLUEPRINT",
        "/opt/openmetadata/catalog/governance-blueprint.yaml",
    )
)


class OpenMetadataClient:
    def __init__(self, host: str, token: str):
        if not token or token.startswith("replace-"):
            raise ValueError("OPENMETADATA_JWT_TOKEN must contain an ingestion-bot JWT")
        self.base_url = host.rstrip("/")
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    def request(
        self,
        method: str,
        path: str,
        payload: Any = None,
        content_type: str = "application/json",
    ) -> dict[str, Any]:
        data = json.dumps(payload).encode() if payload is not None else None
        headers = {**self.headers, "Content-Type": content_type}
        request = Request(
            f"{self.base_url}{path}",
            data=data,
            headers=headers,
            method=method,
        )
        try:
            with urlopen(request, timeout=30) as response:
                body = response.read()
        except HTTPError as exc:
            detail = exc.read().decode(errors="replace")
            raise RuntimeError(f"OpenMetadata {method} {path} failed: {exc.code} {detail}") from exc
        return json.loads(body) if body else {}

    def put(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self.request("PUT", path, payload)

    def get(self, path: str) -> dict[str, Any]:
        return self.request("GET", path)

    def patch(self, path: str, payload: list[dict[str, Any]]) -> dict[str, Any]:
        return self.request("PATCH", path, payload, "application/json-patch+json")


def upsert_definitions(client: OpenMetadataClient, blueprint: dict[str, Any]) -> dict[str, int]:
    counts = {"classifications": 0, "tags": 0, "domains": 0, "data_products": 0, "terms": 0}

    for classification in blueprint["classifications"]:
        client.put(
            "/v1/classifications",
            {
                "name": classification["name"],
                "description": classification["description"],
                "provider": "user",
                "mutuallyExclusive": False,
            },
        )
        counts["classifications"] += 1
        for tag in classification["tags"]:
            client.put(
                "/v1/tags",
                {
                    "classification": classification["name"],
                    "name": tag["name"],
                    "description": tag["description"],
                    "provider": "user",
                },
            )
            counts["tags"] += 1

    for domain in blueprint["domains"]:
        client.put(
            "/v1/domains",
            {
                "name": domain["name"],
                "displayName": domain["display_name"],
                "description": domain["description"],
                "domainType": domain["domain_type"],
            },
        )
        counts["domains"] += 1

    glossary = blueprint["glossary"]
    client.put(
        "/v1/glossaries",
        {
            "name": glossary["name"],
            "description": "Version-controlled business terms for the banking lakehouse pilot.",
            "provider": "user",
        },
    )
    for term in glossary["terms"]:
        client.put(
            "/v1/glossaryTerms",
            {
                "glossary": glossary["name"],
                "name": term["name"],
                "description": term["description"],
                "provider": "user",
            },
        )
        counts["terms"] += 1

    for product in blueprint["data_products"]:
        client.put(
            "/v1/dataProducts",
            {
                "name": product["name"],
                "description": product["description"],
                "domains": [product["domain"]],
            },
        )
        counts["data_products"] += 1

    return counts


def resolve_asset(client: OpenMetadataClient, fqn: str) -> dict[str, str]:
    encoded_fqn = quote(fqn, safe="")
    try:
        entity = client.get(f"/v1/tables/name/{encoded_fqn}")
        return {"id": entity["id"], "type": "table"}
    except RuntimeError as exc:
        if "failed: 404" not in str(exc):
            raise

    table_fqn, _, _ = fqn.rpartition(".")
    if not table_fqn:
        raise RuntimeError(f"Catalog asset not found: {fqn}")
    table = client.get(
        f"/v1/tables/name/{quote(table_fqn, safe='')}?fields=columns,tags"
    )
    if not any(column["fullyQualifiedName"] == fqn for column in table["columns"]):
        raise RuntimeError(f"Catalog asset not found: {fqn}")
    return {
        "id": table["id"],
        "type": "column",
        "table_fqn": table_fqn,
        "column_fqn": fqn,
    }


def apply_column_tags(
    client: OpenMetadataClient,
    column_mappings: list[tuple[dict[str, str], list[str]]],
) -> int:
    mappings_by_table: dict[str, list[tuple[str, list[str]]]] = {}
    for asset, tags in column_mappings:
        mappings_by_table.setdefault(asset["table_fqn"], []).append(
            (asset["column_fqn"], tags)
        )

    applied = 0
    for table_fqn, mappings in mappings_by_table.items():
        table = client.get(
            f"/v1/tables/name/{quote(table_fqn, safe='')}?fields=columns,tags"
        )
        operations: list[dict[str, Any]] = []
        columns = table["columns"]
        for column_fqn, tags in mappings:
            column_index = next(
                index
                for index, column in enumerate(columns)
                if column["fullyQualifiedName"] == column_fqn
            )
            existing_tags = {tag["tagFQN"] for tag in columns[column_index]["tags"]}
            for tag_fqn in tags:
                if tag_fqn in existing_tags:
                    continue
                operations.append(
                    {
                        "op": "add",
                        "path": f"/columns/{column_index}/tags/-",
                        "value": {
                            "tagFQN": tag_fqn,
                            "source": "Classification",
                            "labelType": "Manual",
                            "state": "Confirmed",
                        },
                    }
                )
                applied += 1
        if operations:
            client.patch(f"/v1/tables/{table['id']}?changeSource=Manual", operations)
    return applied


def apply_asset_relationships(
    client: OpenMetadataClient,
    blueprint: dict[str, Any],
) -> dict[str, int]:
    resolved_assets: dict[str, dict[str, str]] = {}
    tag_assets: dict[str, list[dict[str, str]]] = {}
    column_mappings: list[tuple[dict[str, str], list[str]]] = []

    for mapping in blueprint["asset_tags"]:
        asset = resolve_asset(client, mapping["entity"])
        if asset["type"] == "column":
            column_mappings.append((asset, mapping["tags"]))
            continue
        resolved_assets[mapping["entity"]] = asset
        for tag_fqn in mapping["tags"]:
            tag_assets.setdefault(tag_fqn, []).append(asset)

    for tag_fqn, assets in tag_assets.items():
        tag = client.get(f"/v1/tags/name/{quote(tag_fqn, safe='')}")
        client.put(
            f"/v1/tags/{tag['id']}/assets/add",
            {"assets": assets, "dryRun": False},
        )

    column_tag_relationships = apply_column_tags(client, column_mappings)

    for domain in blueprint["domains"]:
        source_schemas = set(domain["source_schemas"])
        assets = [
            asset
            for fqn, asset in resolved_assets.items()
            if len(fqn.split(".")) >= 4 and fqn.split(".")[2] in source_schemas
        ]
        for product in blueprint["data_products"]:
            if product["domain"] != domain["name"]:
                continue
            for fqn in product["output_assets"]:
                asset = resolved_assets.get(fqn) or resolve_asset(client, fqn)
                resolved_assets[fqn] = asset
                if asset not in assets:
                    assets.append(asset)
        if assets:
            client.put(
                f"/v1/domains/{quote(domain['name'], safe='')}/assets/add",
                {"assets": assets, "dryRun": False},
            )

    for product in blueprint["data_products"]:
        assets = []
        for fqn in product["output_assets"]:
            asset = resolved_assets.get(fqn) or resolve_asset(client, fqn)
            resolved_assets[fqn] = asset
            assets.append(asset)
        client.put(
            f"/v1/dataProducts/{quote(product['name'], safe='')}/assets/add",
            {"assets": assets, "dryRun": False},
        )

    return {
        "tagged_assets": len(resolved_assets) + len(column_mappings),
        "table_tag_groups": len(tag_assets),
        "column_tag_relationships": column_tag_relationships,
    }


def main() -> None:
    blueprint = yaml.safe_load(BLUEPRINT_PATH.read_text())
    client = OpenMetadataClient(OPENMETADATA_HOST, JWT_TOKEN)
    definitions = upsert_definitions(client, blueprint)
    relationships = apply_asset_relationships(client, blueprint)
    print(json.dumps({**definitions, **relationships}, sort_keys=True))


if __name__ == "__main__":
    main()
