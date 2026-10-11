"""UI-configured HA camera exporter, installed as a HACS custom integration."""
from __future__ import annotations

import voluptuous as vol

from homeassistant.const import EVENT_HOMEASSISTANT_STOP
from homeassistant.exceptions import ConfigEntryNotReady, ServiceValidationError

from .config_flow import entry_settings, normalize_input
from .const import (
    CONF_ALLOWED_CLIENTS, CONF_BIND_IP, CONF_NAME, CONF_ONVIF_PORT,
    CONF_PASSWORD, CONF_RTSP_PORT, CONF_USERNAME, CONF_VIRTUAL_ID, DOMAIN, VERSION,
)
from .consumer import HACameraConsumer
from .engine import MediaEngine
from .manager import ExportManager
from .onvif_server import ONVIFServer

PLATFORMS = ["button", "switch", "sensor", "binary_sensor"]
SERVICE_SCHEMA = vol.Schema({vol.Required("entry_id"): str})


async def async_setup(hass, config):
    hass.data.setdefault(DOMAIN, {})

    async def control(call):
        manager = hass.data[DOMAIN].get(call.data["entry_id"])
        if manager is None:
            raise ServiceValidationError("Exporter entry is not loaded")
        if call.service == "activate":
            if not await manager.async_activate():
                raise ServiceValidationError("Exporter is disabled or unavailable")
        else:
            await manager.async_deactivate()

    for service in ("activate", "deactivate"):
        if not hass.services.has_service(DOMAIN, service):
            hass.services.async_register(DOMAIN, service, control, schema=SERVICE_SCHEMA)
    return True


async def async_setup_entry(hass, entry):
    if entry.entry_id in hass.data.setdefault(DOMAIN, {}):
        # Failed unload must not bypass the previous consumer/process fence.
        raise ConfigEntryNotReady("Previous exporter cleanup remains unconfirmed")
    settings = normalize_input(entry_settings(entry))
    virtual_id = entry.data[CONF_VIRTUAL_ID]
    consumer = HACameraConsumer(hass, managed_go2rtc=True, receive_audio=False)
    engine = MediaEngine(hass, entry.entry_id, settings)
    onvif = ONVIFServer(
        bind_host=settings[CONF_BIND_IP], port=settings[CONF_ONVIF_PORT],
        advertised_host=settings[CONF_BIND_IP], allowed_peers=settings[CONF_ALLOWED_CLIENTS],
        username=settings[CONF_USERNAME], password=settings[CONF_PASSWORD],
        rtsp_uri=f"rtsp://{settings[CONF_BIND_IP]}:{settings[CONF_RTSP_PORT]}/standby",
        device_id=virtual_id, model=settings[CONF_NAME], firmware_version=VERSION,
    )
    manager = ExportManager(hass, entry.entry_id, settings, consumer, engine, onvif)
    try:
        await manager.start()
    except Exception:
        if not await manager.stop():
            hass.data[DOMAIN][entry.entry_id] = manager
        raise ConfigEntryNotReady("Exporter endpoints could not be started") from None
    entry.runtime_data = manager
    hass.data[DOMAIN][entry.entry_id] = manager

    async def stopping(event):
        await manager.stop()
    entry.async_on_unload(hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, stopping))
    try:
        await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    except BaseException:
        if await manager.stop():
            hass.data[DOMAIN].pop(entry.entry_id, None)
        raise
    return True


async def async_unload_entry(hass, entry):
    manager = entry.runtime_data
    confirmed = await manager.stop()
    if not confirmed:
        return False
    if not await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        return False
    hass.data[DOMAIN].pop(entry.entry_id, None)
    return True
