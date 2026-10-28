"""The Snowflake Exporter integration."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import Any

import aiohttp

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform, __version__ as HA_VERSION
from homeassistant.core import Event, HomeAssistant, callback
from homeassistant.helpers.debounce import Debouncer
from homeassistant.helpers.event import async_track_time_interval

from .const import (
    ATTRIBUTE_DENYLIST,
    CONF_ACCOUNT_LOCATOR,
    CONF_ENTITIES,
    CONF_PRIVATE_KEY,
    CONF_SYNC_INTERVAL,
    CONF_SYNC_MODE,
    DEFAULT_SYNC_INTERVAL,
    DOMAIN,
    SYNC_MODE_ALL,
)
from .exporter import async_write_entity_states, async_write_registry, fqn
from .registry import REGISTRY_EVENTS, collect_registry
from .streaming import SnowflakeRestClient

PLATFORMS = [Platform.SENSOR]

# Registry events arrive in bursts (e.g. at startup), so wait for them to settle
REGISTRY_SYNC_COOLDOWN = 30

_LOGGER = logging.getLogger(__name__)


def _collect_states(hass: HomeAssistant, entry: ConfigEntry) -> list[dict[str, Any]]:
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
            "attributes": {
                k: v for k, v in state.attributes.items()
                if k not in ATTRIBUTE_DENYLIST
            },
            "last_changed": state.last_changed.isoformat(),
            "last_updated": state.last_updated.isoformat(),
        })
    return states


async def _async_ddl_setup(
    client: SnowflakeRestClient,
    session: aiohttp.ClientSession,
    data: dict[str, Any],
    ha_config_payload: dict[str, Any],
) -> None:
    """Create tables and write HA config via SQL API (non-blocking)."""
    config_table = fqn(data, "HA_CONFIGURATION")
    states_table = fqn(data, "HA_ENTITY_STATES")

    try:
        await client.execute_sql(
            session,
            f"CREATE TABLE IF NOT EXISTS {config_table} ("
            "  home_name VARCHAR NOT NULL,"
            "  attributes VARIANT,"
            "  updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())",
            database=data["database"],
            schema=data["schema"],
            warehouse=data["warehouse"],
        )
        home_name = ha_config_payload["home_name"].replace("'", "''")
        attrs_json = json.dumps(ha_config_payload["attributes"]).replace("'", "''")
        await client.execute_sql(
            session,
            f"MERGE INTO {config_table} t "
            f"USING (SELECT '{home_name}' AS home_name, "
            f"PARSE_JSON('{attrs_json}') AS attributes) s "
            "ON t.home_name = s.home_name "
            "WHEN MATCHED THEN UPDATE SET "
            "  t.attributes = s.attributes, t.updated_at = CURRENT_TIMESTAMP() "
            "WHEN NOT MATCHED THEN INSERT (home_name, attributes) "
            "  VALUES (s.home_name, s.attributes)",
            database=data["database"],
            schema=data["schema"],
            warehouse=data["warehouse"],
        )
        _LOGGER.info("Wrote HA configuration to Snowflake")
    except Exception:
        _LOGGER.exception("Failed to write HA configuration to Snowflake")

    try:
        await client.execute_sql(
            session,
            f"CREATE TABLE IF NOT EXISTS {states_table} ("
            "  entity_id       VARCHAR NOT NULL,"
            "  state           VARCHAR,"
            "  attributes      VARIANT,"
            "  last_changed    TIMESTAMP_NTZ,"
            "  last_updated    TIMESTAMP_NTZ,"
            "  replicated_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())",
            database=data["database"],
            schema=data["schema"],
            warehouse=data["warehouse"],
        )
        _LOGGER.info("Ensured HA_ENTITY_STATES table exists")
    except Exception:
        _LOGGER.exception("Failed to create HA_ENTITY_STATES table")


async def _async_sync_registry(hass: HomeAssistant, entry: ConfigEntry) -> None:
    entry_data = hass.data.get(DOMAIN, {}).get(entry.entry_id)
    if not entry_data:
        return
    try:
        counts = await async_write_registry(
            entry_data["client"],
            entry_data["http_session"],
            entry.data,
            collect_registry(hass, entry),
        )
        _LOGGER.info("Synced registry tables to Snowflake: %s", counts)
    except Exception:
        _LOGGER.exception("Failed to sync registry tables to Snowflake")


async def _async_replicate(hass: HomeAssistant, entry: ConfigEntry, _now=None) -> None:
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
            entry_data["client"],
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
    _LOGGER.info("Snowflake Exporter: setting up for %s", data.get("account"))

    client = SnowflakeRestClient(
        account=data["account"],
        account_locator=data.get(CONF_ACCOUNT_LOCATOR, data["account"]),
        user=data["user"],
        private_key_pem=data[CONF_PRIVATE_KEY],
    )
    http_session = aiohttp.ClientSession()

    hass.data[DOMAIN][entry.entry_id] = {
        "config": data,
        "client": client,
        "http_session": http_session,
        "repl_started": None,
        "repl_finished": None,
        "repl_status": None,
        "repl_message": None,
    }

    entry.async_on_unload(entry.add_update_listener(_async_update_listener))

    # DDL setup (background, non-blocking)
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
    hass.async_create_task(
        _async_ddl_setup(client, http_session, data, config_payload)
    )

    # Registry tables: sync now, then again shortly after any registry change
    hass.async_create_task(_async_sync_registry(hass, entry))
    debouncer = Debouncer(
        hass,
        _LOGGER,
        cooldown=REGISTRY_SYNC_COOLDOWN,
        immediate=False,
        function=partial(_async_sync_registry, hass, entry),
    )
    entry.async_on_unload(debouncer.async_cancel)

    @callback
    def _on_registry_updated(_event: Event) -> None:
        hass.async_create_task(debouncer.async_call())

    for event_type in REGISTRY_EVENTS:
        entry.async_on_unload(hass.bus.async_listen(event_type, _on_registry_updated))

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
