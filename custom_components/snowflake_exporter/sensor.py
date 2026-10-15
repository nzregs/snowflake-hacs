"""Sensor platform for Snowflake Exporter."""

from __future__ import annotations

from datetime import timedelta

from homeassistant.components.sensor import SensorDeviceClass, SensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import UnitOfTime
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.util import dt as dt_util

from .const import (
    CONF_ENTITIES,
    CONF_SYNC_INTERVAL,
    CONF_SYNC_MODE,
    DEFAULT_SYNC_INTERVAL,
    DOMAIN,
    SYNC_MODE_ALL,
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    async_add_entities([
        SnowflakeReplicationFrequencySensor(entry),
        SnowflakeReplicationModeSensor(entry),
        SnowflakeReplicationEntityCountSensor(entry),
        SnowflakeNextReplicationSensor(entry),
        SnowflakePrevReplicationStartedSensor(entry),
        SnowflakePrevReplicationFinishedSensor(entry),
        SnowflakePrevReplicationStatusSensor(entry),
        SnowflakePrevReplicationMessageSensor(entry),
    ])


def _device_info(entry: ConfigEntry) -> dict:
    return {
        "identifiers": {(DOMAIN, entry.entry_id)},
        "name": f"Snowflake ({entry.data.get('account', '')})",
        "manufacturer": "Snowflake",
    }


def _entry_data(hass: HomeAssistant, entry: ConfigEntry) -> dict:
    return hass.data.get(DOMAIN, {}).get(entry.entry_id, {})


class SnowflakeReplicationFrequencySensor(SensorEntity):

    _attr_has_entity_name = True
    _attr_name = "Replication Frequency"
    _attr_icon = "mdi:timer-sync-outline"
    _attr_native_unit_of_measurement = UnitOfTime.MINUTES

    def __init__(self, entry: ConfigEntry) -> None:
        self._entry = entry
        self._attr_unique_id = f"{entry.entry_id}_replication_frequency"

    @property
    def device_info(self):
        return _device_info(self._entry)

    @property
    def native_value(self) -> int:
        return self._entry.data.get(CONF_SYNC_INTERVAL, DEFAULT_SYNC_INTERVAL)


class SnowflakeReplicationModeSensor(SensorEntity):

    _attr_has_entity_name = True
    _attr_name = "Replication Mode"
    _attr_icon = "mdi:sync"

    def __init__(self, entry: ConfigEntry) -> None:
        self._entry = entry
        self._attr_unique_id = f"{entry.entry_id}_replication_mode"

    @property
    def device_info(self):
        return _device_info(self._entry)

    @property
    def native_value(self) -> str:
        mode = self._entry.data.get(CONF_SYNC_MODE, SYNC_MODE_ALL)
        return "All Entities" if mode == SYNC_MODE_ALL else "Selected Entities"

    @property
    def extra_state_attributes(self) -> dict:
        data = self._entry.data
        return {
            "account": data.get("account"),
            "database": data.get("database"),
            "schema": data.get("schema"),
            "warehouse": data.get("warehouse"),
        }


class SnowflakeReplicationEntityCountSensor(SensorEntity):

    _attr_has_entity_name = True
    _attr_name = "Replicated Entities"
    _attr_icon = "mdi:counter"
    _attr_native_unit_of_measurement = "entities"

    def __init__(self, entry: ConfigEntry) -> None:
        self._entry = entry
        self._attr_unique_id = f"{entry.entry_id}_replicated_entities"

    @property
    def device_info(self):
        return _device_info(self._entry)

    @property
    def native_value(self) -> int:
        if self._entry.data.get(CONF_SYNC_MODE, SYNC_MODE_ALL) == SYNC_MODE_ALL:
            return len(list(self.hass.states.async_entity_ids()))
        return len(self._entry.data.get(CONF_ENTITIES, []))

    @property
    def extra_state_attributes(self) -> dict:
        data = self._entry.data
        mode = data.get(CONF_SYNC_MODE, SYNC_MODE_ALL)
        if mode == SYNC_MODE_ALL:
            entities = sorted(self.hass.states.async_entity_ids())
        else:
            entities = sorted(data.get(CONF_ENTITIES, []))
        return {"entities": entities}


class SnowflakeNextReplicationSensor(SensorEntity):

    _attr_has_entity_name = True
    _attr_name = "Next Replication"
    _attr_icon = "mdi:calendar-clock"
    _attr_device_class = SensorDeviceClass.TIMESTAMP

    def __init__(self, entry: ConfigEntry) -> None:
        self._entry = entry
        self._attr_unique_id = f"{entry.entry_id}_next_replication"

    @property
    def device_info(self):
        return _device_info(self._entry)

    @property
    def native_value(self):
        ed = _entry_data(self.hass, self._entry)
        finished = ed.get("repl_finished")
        if finished is None:
            return None
        interval = self._entry.data.get(CONF_SYNC_INTERVAL, DEFAULT_SYNC_INTERVAL)
        return finished + timedelta(minutes=interval)


class SnowflakePrevReplicationStartedSensor(SensorEntity):

    _attr_has_entity_name = True
    _attr_name = "Previous Replication Started"
    _attr_icon = "mdi:clock-start"
    _attr_device_class = SensorDeviceClass.TIMESTAMP

    def __init__(self, entry: ConfigEntry) -> None:
        self._entry = entry
        self._attr_unique_id = f"{entry.entry_id}_prev_repl_started"

    @property
    def device_info(self):
        return _device_info(self._entry)

    @property
    def native_value(self):
        return _entry_data(self.hass, self._entry).get("repl_started")


class SnowflakePrevReplicationFinishedSensor(SensorEntity):

    _attr_has_entity_name = True
    _attr_name = "Previous Replication Finished"
    _attr_icon = "mdi:clock-check-outline"
    _attr_device_class = SensorDeviceClass.TIMESTAMP

    def __init__(self, entry: ConfigEntry) -> None:
        self._entry = entry
        self._attr_unique_id = f"{entry.entry_id}_prev_repl_finished"

    @property
    def device_info(self):
        return _device_info(self._entry)

    @property
    def native_value(self):
        return _entry_data(self.hass, self._entry).get("repl_finished")


class SnowflakePrevReplicationStatusSensor(SensorEntity):

    _attr_has_entity_name = True
    _attr_name = "Previous Replication Status"
    _attr_icon = "mdi:check-circle-outline"

    def __init__(self, entry: ConfigEntry) -> None:
        self._entry = entry
        self._attr_unique_id = f"{entry.entry_id}_prev_repl_status"

    @property
    def device_info(self):
        return _device_info(self._entry)

    @property
    def native_value(self) -> str | None:
        return _entry_data(self.hass, self._entry).get("repl_status")

    @property
    def icon(self) -> str:
        status = _entry_data(self.hass, self._entry).get("repl_status")
        if status == "Success":
            return "mdi:check-circle-outline"
        if status == "Failed":
            return "mdi:alert-circle-outline"
        return "mdi:help-circle-outline"


class SnowflakePrevReplicationMessageSensor(SensorEntity):

    _attr_has_entity_name = True
    _attr_name = "Previous Replication Message"
    _attr_icon = "mdi:message-text-outline"

    def __init__(self, entry: ConfigEntry) -> None:
        self._entry = entry
        self._attr_unique_id = f"{entry.entry_id}_prev_repl_message"

    @property
    def device_info(self):
        return _device_info(self._entry)

    @property
    def native_value(self) -> str | None:
        return _entry_data(self.hass, self._entry).get("repl_message")
