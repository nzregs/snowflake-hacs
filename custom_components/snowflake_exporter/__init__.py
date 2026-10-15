"""The Snowflake Exporter integration."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import Any

import aiohttp
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key
import snowflake.connector

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform, __version__ as HA_VERSION
from homeassistant.core import HomeAssistant
from homeassistant.helpers.event import async_track_time_interval

from .const import (
    CONF_ACCOUNT_LOCATOR,
    CONF_ENTITIES,
    CONF_PRIVATE_KEY,
    CONF_SYNC_INTERVAL,
    CONF_SYNC_MODE,
    DEFAULT_SYNC_INTERVAL,
    DOMAIN,
    SYNC_MODE_ALL,
)
from .exporter import async_write_entity_states, ensure_entity_states_table
from .streaming import SnowflakeStreamingClient

PLATFORMS = [Platform.SENSOR]

_LOGGER = logging.getLogger(__name__)


def _get_connection(data: dict[str, Any], pk_der: bytes) -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(
        account=data["account"],
        user=data["user"],
        private_key=pk_der,
        database=data["database"],
        schema=data["schema"],
        warehouse=data["warehouse"],
        login_timeout=10,
    )


def _fqn(data: dict[str, Any], table: str) -> str:
    return f"{data['database']}.{data['schema']}.{table}"


def _write_ha_config(
    data: dict[str, Any], pk_der: bytes, hass_config: dict[str, Any]
) -> None:
    conn = _get_connection(data, pk_der)
    table = _fqn(data, "HA_CONFIGURATION")
    try:
        cur = conn.cursor()
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {table} ("
            "  home_name VARCHAR NOT NULL,"
            "  attributes VARIANT,"
            "  updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()"
            ")"
        )
        cur.execute(
            f"MERGE INTO {table} t "
            "USING (SELECT %s AS home_name, PARSE_JSON(%s) AS attributes) s "
            "ON t.home_name = s.home_name "
            "WHEN MATCHED THEN UPDATE SET "
            "  t.attributes = s.attributes, t.updated_at = CURRENT_TIMESTAMP() "
            "WHEN NOT MATCHED THEN INSERT (home_name, attributes) "
            "  VALUES (s.home_name, s.attributes)",
            (hass_config["home_name"], json.dumps(hass_config["attributes"])),
        )
        cur.close()
    finally:
        conn.close()


def _collect_states(hass: HomeAssistant, entry: ConfigEntry) -> list[dict[str, Any]]:
    """Collect current entity states based on sync mode config."""
    data = entry.data
    sync_mode = data.get(CONF_SYNC_MODE, SYNC_MODE_ALL)

    if sync_mode == SYNC_MODE_ALL:
        entity_ids = list(hass.states.async_entity_ids())
    else:
        entity_ids = data.get(CONF_ENTITIES, [])

    states = []
    for eid in entity_ids:
        state = hass.states.get(eid)
        if state is None:
            continue
        states.append({
            "entity_id": eid,
            "state": state.state,
            "attributes": dict(state.attributes),
            "last_changed": state.last_changed.isoformat(),
            "last_updated": state.last_updated.isoformat(),
        })
    return states


async def _async_replicate(hass: HomeAssistant, entry: ConfigEntry, _now=None) -> None:
    """Replicate entity states to Snowflake via Streaming REST API."""
    entry_data = hass.data.get(DOMAIN, {}).get(entry.entry_id)
    if not entry_data:
        return

    started = datetime.now(timezone.utc)
    entry_data["repl_started"] = started
    entry_data["repl_status"] = "Running"
    entry_data["repl_message"] = "Replication in progress..."

    states = _collect_states(hass, entry)
    try:
        count = await async_write_entity_states(
            entry_data["streaming_client"],
            entry_data["http_session"],
            entry.data,
            states,
        )
        finished = datetime.now(timezone.utc)
        entry_data["repl_finished"] = finished
        entry_data["repl_status"] = "Success"
        entry_data["repl_message"] = (
            f"Streamed {count} records for {len(states)} entities"
        )
        _LOGGER.info("Streamed %d entity states to Snowflake", count)
    except Exception as err:
        finished = datetime.now(timezone.utc)
        entry_data["repl_finished"] = finished
        entry_data["repl_status"] = "Failed"
        entry_data["repl_message"] = str(err)[:255]
        _LOGGER.exception("Failed to stream entity states to Snowflake")


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    hass.data.setdefault(DOMAIN, {})

    data = entry.data
    _LOGGER.info("Snowflake Exporter: setup starting for %s", data.get("account"))
    pk = load_pem_private_key(data[CONF_PRIVATE_KEY].encode(), password=None)
    pk_der = pk.private_bytes(
        serialization.Encoding.DER,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    conn_factory = partial(_get_connection, data, pk_der)

    # Migrate: fetch account locator if missing from older config entries
    if CONF_ACCOUNT_LOCATOR not in data:
        try:
            def _get_locator():
                conn = conn_factory()
                cur = conn.cursor()
                cur.execute("SELECT CURRENT_ACCOUNT()")
                locator = cur.fetchone()[0]
                cur.close()
                conn.close()
                return locator

            locator = await hass.async_add_executor_job(_get_locator)
            hass.config_entries.async_update_entry(
                entry, data={**data, CONF_ACCOUNT_LOCATOR: locator}
            )
            data = entry.data
            _LOGGER.info("Stored account locator %s for streaming API", locator)
        except Exception:
            _LOGGER.exception("Failed to fetch account locator")

    # Create streaming client and HTTP session
    streaming_client = SnowflakeStreamingClient(
        account=data["account"],
        account_locator=data.get(CONF_ACCOUNT_LOCATOR, data["account"]),
        user=data["user"],
        private_key_pem=data[CONF_PRIVATE_KEY],
    )
    http_session = aiohttp.ClientSession()

    hass.data[DOMAIN][entry.entry_id] = {
        "config": data,
        "private_key_der": pk_der,
        "streaming_client": streaming_client,
        "http_session": http_session,
        "repl_started": None,
        "repl_finished": None,
        "repl_status": None,
        "repl_message": None,
    }

    entry.async_on_unload(entry.add_update_listener(_async_update_listener))

    # DDL: create tables and write config via connector (background, non-blocking)
    async def _async_ddl_setup():
        ha_config = hass.config
        config_payload = {
            "home_name": ha_config.location_name,
            "attributes": {
                "ha_version": HA_VERSION,
                "time_zone": str(ha_config.time_zone),
                "latitude": ha_config.latitude,
                "longitude": ha_config.longitude,
                "elevation": ha_config.elevation,
                "unit_system": str(ha_config.units),
                "currency": ha_config.currency,
                "country": ha_config.country,
                "language": ha_config.language,
            },
        }
        try:
            await hass.async_add_executor_job(
                _write_ha_config, data, pk_der, config_payload
            )
            _LOGGER.info("Wrote HA configuration to Snowflake")
        except Exception:
            _LOGGER.exception("Failed to write HA configuration to Snowflake")

        try:
            await hass.async_add_executor_job(ensure_entity_states_table, conn_factory, data)
            _LOGGER.info("Ensured HA_ENTITY_STATES table exists")
        except Exception:
            _LOGGER.exception("Failed to create HA_ENTITY_STATES table")

    hass.async_create_task(_async_ddl_setup())

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    # Run first replication immediately
    await _async_replicate(hass, entry)

    # Schedule periodic replication
    interval = data.get(CONF_SYNC_INTERVAL, DEFAULT_SYNC_INTERVAL)
    unsub = async_track_time_interval(
        hass,
        partial(_async_replicate, hass, entry),
        timedelta(minutes=interval),
    )
    hass.data[DOMAIN][entry.entry_id]["unsub_timer"] = unsub

    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    entry_data = hass.data.get(DOMAIN, {}).get(entry.entry_id)
    if entry_data:
        if "unsub_timer" in entry_data:
            entry_data["unsub_timer"]()
        if "http_session" in entry_data:
            await entry_data["http_session"].close()

    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        hass.data[DOMAIN].pop(entry.entry_id, None)
    return unload_ok


async def _async_update_listener(hass: HomeAssistant, entry: ConfigEntry) -> None:
    await hass.config_entries.async_reload(entry.entry_id)
