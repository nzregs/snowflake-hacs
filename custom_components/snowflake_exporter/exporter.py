"""Snowflake write logic for entity state replication."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

import snowflake.connector

_LOGGER = logging.getLogger(__name__)


def _fqn(data: dict[str, Any], table: str) -> str:
    return f"{data['database']}.{data['schema']}.{table}"


def _ensure_entity_states_table(
    cur: snowflake.connector.cursor.SnowflakeCursor, fqn: str
) -> None:
    cur.execute(
        f"CREATE TABLE IF NOT EXISTS {fqn} ("
        "  entity_id       VARCHAR NOT NULL,"
        "  state           VARCHAR,"
        "  attributes      VARIANT,"
        "  last_changed    TIMESTAMP_NTZ,"
        "  last_updated    TIMESTAMP_NTZ,"
        "  replicated_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()"
        ")"
    )


def write_entity_states(
    conn_factory,
    data: dict[str, Any],
    states: list[dict[str, Any]],
) -> int:
    """Write entity states to Snowflake. Returns number of rows inserted."""
    if not states:
        return 0

    conn = conn_factory()
    table = _fqn(data, "HA_ENTITY_STATES")
    try:
        cur = conn.cursor()
        _ensure_entity_states_table(cur, table)

        for s in states:
            cur.execute(
                f"INSERT INTO {table} "
                "(entity_id, state, attributes, last_changed, last_updated) "
                "SELECT %s, %s, PARSE_JSON(%s), %s, %s",
                (
                    s["entity_id"],
                    s["state"],
                    json.dumps(s["attributes"], default=str),
                    s["last_changed"],
                    s["last_updated"],
                ),
            )

        cur.close()
        _LOGGER.debug("Inserted %d entity states into Snowflake", len(states))
        return len(states)
    finally:
        conn.close()
