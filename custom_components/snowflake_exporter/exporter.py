"""Snowflake write logic for entity state replication."""

from __future__ import annotations

import json
import logging
from typing import Any

import aiohttp
import snowflake.connector

from .streaming import SnowflakeStreamingClient

_LOGGER = logging.getLogger(__name__)


def _fqn(data: dict[str, Any], table: str) -> str:
    return f"{data['database']}.{data['schema']}.{table}"


def ensure_entity_states_table(
    conn_factory, data: dict[str, Any]
) -> None:
    """Create HA_ENTITY_STATES table if it doesn't exist (DDL via connector)."""
    conn = conn_factory()
    table = _fqn(data, "HA_ENTITY_STATES")
    try:
        cur = conn.cursor()
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {table} ("
            "  entity_id       VARCHAR NOT NULL,"
            "  state           VARCHAR,"
            "  attributes      VARIANT,"
            "  last_changed    TIMESTAMP_NTZ,"
            "  last_updated    TIMESTAMP_NTZ,"
            "  replicated_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()"
            ")"
        )
        cur.close()
    finally:
        conn.close()


async def async_write_entity_states(
    client: SnowflakeStreamingClient,
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
