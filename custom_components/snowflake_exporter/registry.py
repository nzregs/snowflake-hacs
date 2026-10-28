"""Collect Home Assistant entity, device and area registry data for export."""

from __future__ import annotations

import ipaddress
from typing import Any
from urllib.parse import urlparse

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers import (
    area_registry as ar,
    device_registry as dr,
    entity_registry as er,
)

try:  # Floors were added in Home Assistant 2024.4
    from homeassistant.helpers import floor_registry as fr
except ImportError:  # pragma: no cover
    fr = None

from .const import CONF_ENTITIES, CONF_SYNC_MODE, SYNC_MODE_ALL

# Registry events that should trigger a re-sync
REGISTRY_EVENTS = [
    er.EVENT_ENTITY_REGISTRY_UPDATED,
    dr.EVENT_DEVICE_REGISTRY_UPDATED,
    ar.EVENT_AREA_REGISTRY_UPDATED,
]
if fr is not None:
    REGISTRY_EVENTS.append(fr.EVENT_FLOOR_REGISTRY_UPDATED)


def _enum(value: Any) -> str | None:
    if value is None:
        return None
    return getattr(value, "value", str(value))


def _labels(obj: Any) -> list[str]:
    return sorted(getattr(obj, "labels", None) or [])


def _ip_from_url(url: str | None) -> str | None:
    if not url:
        return None
    host = urlparse(url).hostname
    if not host:
        return None
    try:
        return str(ipaddress.ip_address(host))
    except ValueError:
        return None


def collect_registry(
    hass: HomeAssistant, entry: ConfigEntry
) -> dict[str, list[dict[str, Any]]]:
    """Return entity, device and area rows for the entities this entry syncs."""
    ent_reg = er.async_get(hass)
    dev_reg = dr.async_get(hass)
    area_reg = ar.async_get(hass)
    floor_reg = fr.async_get(hass) if fr is not None else None

    if entry.data.get(CONF_SYNC_MODE, SYNC_MODE_ALL) == SYNC_MODE_ALL:
        # Registry entries plus entities that only exist as states (e.g. YAML without unique_id)
        entity_ids = set(ent_reg.entities) | set(hass.states.async_entity_ids())
    else:
        entity_ids = set(entry.data.get(CONF_ENTITIES, []))

    entities: list[dict[str, Any]] = []
    device_ids: set[str] = set()
    area_ids: set[str] = set()

    for entity_id in sorted(entity_ids):
        reg = ent_reg.async_get(entity_id)
        state = hass.states.get(entity_id)
        if reg is None and state is None:
            continue

        device = dev_reg.async_get(reg.device_id) if reg and reg.device_id else None
        area_id = (reg.area_id if reg else None) or (device.area_id if device else None)

        config_entry = (
            hass.config_entries.async_get_entry(reg.config_entry_id)
            if reg and reg.config_entry_id
            else None
        )
        device_class = (
            (reg.device_class or reg.original_device_class) if reg else None
        ) or (state.attributes.get("device_class") if state else None)
        name = ((reg.name or reg.original_name) if reg else None) or (
            state.attributes.get("friendly_name") if state else None
        )

        entities.append({
            "entity_id": entity_id,
            "domain": entity_id.split(".", 1)[0],
            "unique_id": reg.unique_id if reg else None,
            "platform": reg.platform if reg else None,
            "integration": config_entry.domain if config_entry else (reg.platform if reg else None),
            "config_entry_title": config_entry.title if config_entry else None,
            "device_id": device.id if device else None,
            "area_id": area_id,
            "name": name,
            "device_class": _enum(device_class),
            "unit_of_measurement": (
                reg.unit_of_measurement if reg else None
            ) or (state.attributes.get("unit_of_measurement") if state else None),
            "entity_category": _enum(reg.entity_category) if reg else None,
            "disabled_by": _enum(reg.disabled_by) if reg else None,
            "hidden_by": _enum(reg.hidden_by) if reg else None,
            "labels": _labels(reg),
        })
        if device:
            device_ids.add(device.id)
        if area_id:
            area_ids.add(area_id)

    # Include parent devices (via_device_id) so hub/bridge relationships resolve
    pending = list(device_ids)
    while pending:
        device = dev_reg.async_get(pending.pop())
        if device and device.via_device_id and device.via_device_id not in device_ids:
            device_ids.add(device.via_device_id)
            pending.append(device.via_device_id)

    devices: list[dict[str, Any]] = []
    for device_id in sorted(device_ids):
        device = dev_reg.async_get(device_id)
        if device is None:
            continue
        if device.area_id:
            area_ids.add(device.area_id)
        macs = [v for t, v in device.connections if t == dr.CONNECTION_NETWORK_MAC]
        integrations = sorted({
            ce.domain
            for ce_id in device.config_entries
            if (ce := hass.config_entries.async_get_entry(ce_id))
        })
        devices.append({
            "device_id": device.id,
            "name": device.name,
            "name_by_user": device.name_by_user,
            "manufacturer": device.manufacturer,
            "model": device.model,
            "model_id": getattr(device, "model_id", None),
            "serial_number": getattr(device, "serial_number", None),
            "sw_version": device.sw_version,
            "hw_version": device.hw_version,
            "area_id": device.area_id,
            "via_device_id": device.via_device_id,
            "mac_address": macs[0] if macs else None,
            "ip_address": _ip_from_url(device.configuration_url),
            "configuration_url": device.configuration_url,
            "connections": [list(c) for c in sorted(device.connections)],
            "identifiers": [list(i) for i in sorted(device.identifiers)],
            "integrations": integrations,
            "entry_type": _enum(device.entry_type),
            "disabled_by": _enum(device.disabled_by),
            "labels": _labels(device),
        })

    areas: list[dict[str, Any]] = []
    for area_id in sorted(area_ids):
        area = area_reg.async_get_area(area_id)
        if area is None:
            continue
        floor_id = getattr(area, "floor_id", None)
        floor = floor_reg.async_get_floor(floor_id) if floor_reg and floor_id else None
        areas.append({
            "area_id": area.id,
            "name": area.name,
            "floor_id": floor_id,
            "floor_name": floor.name if floor else None,
            "floor_level": floor.level if floor else None,
            "labels": _labels(area),
        })

    return {"entities": entities, "devices": devices, "areas": areas}
