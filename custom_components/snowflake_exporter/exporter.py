"""Snowflake write logic for entity state and registry replication."""

from __future__ import annotations

import json
import logging
import re
from typing import Any

import aiohttp

from .const import IDENTIFIER_FIELDS, IDENTIFIER_PATTERN
from .streaming import SnowflakeRestClient

_LOGGER = logging.getLogger(__name__)

# kind -> (table, key column, [(column, Snowflake type), ...]). Each column name is also the JSON key.
REGISTRY_TABLES: dict[str, tuple[str, str, list[tuple[str, str]]]] = {
    "entities": ("HA_ENTITIES", "entity_id", [
        ("entity_id", "VARCHAR"),
        ("domain", "VARCHAR"),
        ("unique_id", "VARCHAR"),
        ("platform", "VARCHAR"),
        ("integration", "VARCHAR"),
        ("config_entry_title", "VARCHAR"),
        ("device_id", "VARCHAR"),
        ("area_id", "VARCHAR"),
        ("name", "VARCHAR"),
        ("device_class", "VARCHAR"),
        ("unit_of_measurement", "VARCHAR"),
        ("entity_category", "VARCHAR"),
        ("disabled_by", "VARCHAR"),
        ("hidden_by", "VARCHAR"),
        ("labels", "ARRAY"),
    ]),
    "devices": ("HA_DEVICES", "device_id", [
        ("device_id", "VARCHAR"),
        ("name", "VARCHAR"),
        ("name_by_user", "VARCHAR"),
        ("manufacturer", "VARCHAR"),
        ("model", "VARCHAR"),
        ("model_id", "VARCHAR"),
        ("serial_number", "VARCHAR"),
        ("sw_version", "VARCHAR"),
        ("hw_version", "VARCHAR"),
        ("area_id", "VARCHAR"),
        ("via_device_id", "VARCHAR"),
        ("mac_address", "VARCHAR"),
        ("ip_address", "VARCHAR"),
        ("configuration_url", "VARCHAR"),
        ("connections", "ARRAY"),
        ("identifiers", "ARRAY"),
        ("integrations", "ARRAY"),
        ("entry_type", "VARCHAR"),
        ("disabled_by", "VARCHAR"),
        ("labels", "ARRAY"),
    ]),
    "areas": ("HA_AREAS", "area_id", [
        ("area_id", "VARCHAR"),
        ("name", "VARCHAR"),
        ("floor_id", "VARCHAR"),
        ("floor_name", "VARCHAR"),
        ("floor_level", "NUMBER"),
        ("labels", "ARRAY"),
    ]),
}


def fqn(data: dict[str, Any], table: str) -> str:
    for field in IDENTIFIER_FIELDS:
        if not re.fullmatch(IDENTIFIER_PATTERN, data[field]):
            raise ValueError(f"Invalid Snowflake identifier for {field}: {data[field]!r}")
    return f"{data['database']}.{data['schema']}.{table}"


async def async_write_entity_states(
    client: SnowflakeRestClient,
    session: aiohttp.ClientSession,
    data: dict[str, Any],
    states: list[dict[str, Any]],
) -> int:
    """Write entity states via Snowpipe Streaming Elastic Channel REST API."""
    if not states:
        return 0

    rows = []
    for s in states:
        rows.append({
            "entity_id": s["entity_id"],
            "state": s["state"],
            "attributes": json.dumps(s["attributes"], default=str),
            "last_changed": s["last_changed"],
            "last_updated": s["last_updated"],
        })

    return await client.send_rows(
        session,
        data["database"],
        data["schema"],
        "HA_ENTITY_STATES",
        rows,
    )


async def async_write_registry(
    client: SnowflakeRestClient,
    session: aiohttp.ClientSession,
    data: dict[str, Any],
    snapshot: dict[str, list[dict[str, Any]]],
) -> dict[str, int]:
    """Replace the registry tables' contents with the snapshot via the SQL API.

    Rows are merged on their key column, and rows no longer in the snapshot are deleted.
    The snapshot is passed as a bound JSON parameter, never interpolated into the SQL.
    """
    sql_kwargs = {
        "database": data["database"],
        "schema": data["schema"],
        "warehouse": data["warehouse"],
    }
    counts: dict[str, int] = {}

    for kind, (table, key, columns) in REGISTRY_TABLES.items():
        target = fqn(data, table)
        rows = snapshot.get(kind, [])
        binding = {"1": {"type": "TEXT", "value": json.dumps(rows, default=str)}}
        col_names = [c for c, _ in columns]

        await client.execute_sql(
            session,
            f"CREATE TABLE IF NOT EXISTS {target} ("
            + ", ".join(f"{c} {t}" for c, t in columns)
            + ", updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())",
            **sql_kwargs,
        )

        source = (
            "SELECT "
            + ", ".join(f'f.value:"{c}"::{t} AS {c}' for c, t in columns)
            + " FROM TABLE(FLATTEN(INPUT => PARSE_JSON(?))) f"
        )
        await client.execute_sql(
            session,
            f"MERGE INTO {target} t USING ({source}) s ON t.{key} = s.{key} "
            "WHEN MATCHED THEN UPDATE SET "
            + ", ".join(f"t.{c} = s.{c}" for c in col_names if c != key)
            + ", t.updated_at = CURRENT_TIMESTAMP() "
            f"WHEN NOT MATCHED THEN INSERT ({', '.join(col_names)}) "
            f"VALUES ({', '.join(f's.{c}' for c in col_names)})",
            bindings=binding,
            **sql_kwargs,
        )

        await client.execute_sql(
            session,
            f"DELETE FROM {target} WHERE {key} NOT IN ("
            f'SELECT f.value:"{key}"::VARCHAR FROM TABLE(FLATTEN(INPUT => PARSE_JSON(?))) f)',
            bindings=binding,
            **sql_kwargs,
        )
        counts[table] = len(rows)

    return counts
