"""Snowflake write logic for entity state replication."""

from __future__ import annotations

import json
import logging
from typing import Any

import aiohttp

from .streaming import SnowflakeRestClient

_LOGGER = logging.getLogger(__name__)


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
