"""UI configuration for a camera exposed as a private ONVIF device."""

from __future__ import annotations

import errno
import json
import math
import re
import socket
from copy import deepcopy
from ipaddress import IPv4Address
from typing import Any
from uuid import uuid4

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.components import network
from homeassistant.core import callback
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import selector

from .const import (
    CONFIG_FIELDS,
    CONF_ALLOWED_CLIENTS,
    CONF_BINARY_SENSOR,
    CONF_BIND_IP,
    CONF_CAMERA_ENTITY,
    CONF_EVENT_AT_FIELD,
    CONF_EVENT_FILTER,
    CONF_EVENT_TYPE,
    CONF_NAME,
    CONF_ONVIF_PORT,
    CONF_PASSWORD,
    CONF_RTSP_PORT,
    CONF_TRIGGER_MODE,
    CONF_USERNAME,
    CONF_VIRTUAL_ID,
    CONF_WINDOW_SECONDS,
    DEFAULT_ALLOWED_CLIENTS,
    DEFAULT_BIND_IP,
    DEFAULT_EVENT_AT_FIELD,
    DEFAULT_EVENT_TYPE,
    DEFAULT_NAME,
    DEFAULT_ONVIF_PORT,
    DEFAULT_RTSP_PORT,
    DEFAULT_TRIGGER_MODE,
    DEFAULT_USERNAME,
    DEFAULT_WINDOW_SECONDS,
    DOMAIN,
    MAX_ALLOWED_CLIENTS,
    MAX_EVENT_FILTER_BYTES,
    MAX_EVENT_FILTER_FIELDS,
    MAX_EVENT_FILTER_VALUES,
    MAX_PORT,
    MAX_WINDOW_SECONDS,
    MIN_PORT,
    MIN_WINDOW_SECONDS,
    TRIGGER_BINARY_SENSOR,
    TRIGGER_EVENTS,
    TRIGGER_MODES,
)

_FIELD_RE = re.compile(r"[A-Za-z][A-Za-z0-9_]{0,63}\Z")
_EVENT_RE = re.compile(r"[A-Za-z][A-Za-z0-9_.:-]{0,127}\Z")
_ENTITY_RE = re.compile(r"(?:camera|binary_sensor)\.[a-z0-9_]+\Z")
_USERNAME_RE = re.compile(r"[A-Za-z0-9_.-]{1,64}\Z")
_DEFAULTS = {
    CONF_NAME: DEFAULT_NAME,
    CONF_WINDOW_SECONDS: DEFAULT_WINDOW_SECONDS,
    CONF_BIND_IP: DEFAULT_BIND_IP,
    CONF_ONVIF_PORT: DEFAULT_ONVIF_PORT,
    CONF_RTSP_PORT: DEFAULT_RTSP_PORT,
    CONF_USERNAME: DEFAULT_USERNAME,
    CONF_ALLOWED_CLIENTS: list(DEFAULT_ALLOWED_CLIENTS),
    CONF_TRIGGER_MODE: DEFAULT_TRIGGER_MODE,
    CONF_EVENT_TYPE: DEFAULT_EVENT_TYPE,
    CONF_EVENT_FILTER: {},
    CONF_EVENT_AT_FIELD: DEFAULT_EVENT_AT_FIELD,
}


class ValidationError(Exception):
    """Only safe, translatable field/code pairs cross the UI boundary."""

    def __init__(self, field: str, code: str) -> None:
        super().__init__(code)
        self.field = field
        self.code = code


def entry_settings(entry: Any) -> dict[str, Any]:
    """Return settings without exposing or accepting changes to virtual identity."""
    merged = dict(entry.data) | dict(entry.options)
    return {key: value for key, value in merged.items() if key in CONFIG_FIELDS}


def _ipv4(value: Any, field: str) -> str:
    if not isinstance(value, str) or value != value.strip():
        raise ValidationError(field, "invalid_ip")
    try:
        ip = IPv4Address(value)
    except ValueError:
        raise ValidationError(field, "invalid_ip") from None
    if ip.is_unspecified or ip.is_multicast or ip == IPv4Address("255.255.255.255"):
        raise ValidationError(field, "invalid_ip")
    return str(ip)


def _integer(value: Any, field: str, minimum: int, maximum: int) -> int:
    # NumberSelector may return an integral float. bool must never be an integer.
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(field, "invalid_number")
    if not minimum <= value <= maximum or not math.isfinite(value) or value != int(value):
        raise ValidationError(field, "invalid_number")
    return int(value)


def normalize_allowed_clients(value: Any) -> list[str]:
    """Permit only a non-empty list of exact IPv4 peers, never ranges/CIDRs."""
    if isinstance(value, str):
        if len(value) > MAX_ALLOWED_CLIENTS * 32:
            raise ValidationError(CONF_ALLOWED_CLIENTS, "invalid_clients")
        peers = [line.strip() for line in value.splitlines() if line.strip()]
    elif isinstance(value, list):
        peers = value
    else:
        raise ValidationError(CONF_ALLOWED_CLIENTS, "invalid_clients")
    if not 1 <= len(peers) <= MAX_ALLOWED_CLIENTS:
        raise ValidationError(CONF_ALLOWED_CLIENTS, "invalid_clients")
    result: list[str] = []
    for peer in peers:
        try:
            parsed = _ipv4(peer, CONF_ALLOWED_CLIENTS)
        except ValidationError:
            raise ValidationError(CONF_ALLOWED_CLIENTS, "invalid_clients") from None
        if parsed not in result:
            result.append(parsed)
    return result


def normalize_event_filter(value: Any) -> dict[str, Any]:
    """A flat AND filter; a non-empty list at one key means OR at that key."""
    if not isinstance(value, dict) or len(value) > MAX_EVENT_FILTER_FIELDS:
        raise ValidationError(CONF_EVENT_FILTER, "invalid_filter")
    result: dict[str, Any] = {}
    for key, allowed in value.items():
        if not isinstance(key, str) or not _FIELD_RE.fullmatch(key):
            raise ValidationError(CONF_EVENT_FILTER, "invalid_filter")
        members = allowed if isinstance(allowed, list) else [allowed]
        if not 1 <= len(members) <= MAX_EVENT_FILTER_VALUES:
            raise ValidationError(CONF_EVENT_FILTER, "invalid_filter")
        for member in members:
            if member is None or isinstance(member, bool):
                continue
            if type(member) is int and -(2**53) < member < 2**53:
                continue
            if isinstance(member, str) and len(member) <= 256 and not any(
                ord(char) < 32 for char in member
            ):
                try:
                    member.encode("utf-8", errors="strict")
                except UnicodeError:
                    raise ValidationError(CONF_EVENT_FILTER, "invalid_filter") from None
                continue
            raise ValidationError(CONF_EVENT_FILTER, "invalid_filter")
        result[key] = deepcopy(allowed)
    if len(json.dumps(result, ensure_ascii=False).encode("utf-8")) > MAX_EVENT_FILTER_BYTES:
        raise ValidationError(CONF_EVENT_FILTER, "invalid_filter")
    return result


def normalize_input(raw: dict[str, Any], current: dict[str, Any] | None = None) -> dict[str, Any]:
    """Validate untrusted UI input, retaining a password only on explicit edit flow."""
    if not isinstance(raw, dict) or set(raw) - CONFIG_FIELDS:
        raise ValidationError("base", "invalid_input")
    data = deepcopy(_DEFAULTS) | deepcopy(current or {}) | deepcopy(raw)
    if current is not None and raw.get(CONF_PASSWORD, "") == "":
        data[CONF_PASSWORD] = current.get(CONF_PASSWORD, "")

    for field, limit in ((CONF_NAME, 100), (CONF_PASSWORD, 128)):
        value = data.get(field)
        if not isinstance(value, str) or not value or len(value) > limit or any(
            ord(char) < 32 or ord(char) == 127 for char in value
        ):
            raise ValidationError(field, "invalid_text" if field == CONF_NAME else "invalid_password")
    data[CONF_NAME] = data[CONF_NAME].strip()
    if not data[CONF_NAME]:
        raise ValidationError(CONF_NAME, "invalid_text")
    try:
        data[CONF_NAME].encode("utf-8", errors="strict")
    except UnicodeError:
        raise ValidationError(CONF_NAME, "invalid_text") from None
    # ASCII dedicated credentials keep Basic/RTSP clients interoperable.
    password = data[CONF_PASSWORD]
    if len(password) < 16 or not password.isascii():
        raise ValidationError(CONF_PASSWORD, "invalid_password")
    if not isinstance(data.get(CONF_USERNAME), str) or not _USERNAME_RE.fullmatch(data[CONF_USERNAME]) or data[CONF_USERNAME] in {"any", "window_publisher"}:
        raise ValidationError(CONF_USERNAME, "invalid_username")
    data[CONF_BIND_IP] = _ipv4(data.get(CONF_BIND_IP), CONF_BIND_IP)
    data[CONF_ALLOWED_CLIENTS] = normalize_allowed_clients(data.get(CONF_ALLOWED_CLIENTS))
    data[CONF_WINDOW_SECONDS] = _integer(data[CONF_WINDOW_SECONDS], CONF_WINDOW_SECONDS, MIN_WINDOW_SECONDS, MAX_WINDOW_SECONDS)
    for field in (CONF_ONVIF_PORT, CONF_RTSP_PORT):
        data[field] = _integer(data[field], field, MIN_PORT, MAX_PORT)
    if data[CONF_ONVIF_PORT] == data[CONF_RTSP_PORT]:
        raise ValidationError(CONF_RTSP_PORT, "same_port")
    if data.get(CONF_TRIGGER_MODE) not in TRIGGER_MODES:
        raise ValidationError(CONF_TRIGGER_MODE, "invalid_trigger")

    camera = data.get(CONF_CAMERA_ENTITY)
    if not isinstance(camera, str) or not _ENTITY_RE.fullmatch(camera) or not camera.startswith("camera."):
        raise ValidationError(CONF_CAMERA_ENTITY, "invalid_entity")
    if data[CONF_TRIGGER_MODE] == TRIGGER_EVENTS:
        if not isinstance(data.get(CONF_EVENT_TYPE), str) or not _EVENT_RE.fullmatch(data[CONF_EVENT_TYPE]):
            raise ValidationError(CONF_EVENT_TYPE, "invalid_event")
        if not isinstance(data.get(CONF_EVENT_AT_FIELD), str) or not _FIELD_RE.fullmatch(data[CONF_EVENT_AT_FIELD]):
            raise ValidationError(CONF_EVENT_AT_FIELD, "invalid_event")
        data[CONF_EVENT_FILTER] = normalize_event_filter(data.get(CONF_EVENT_FILTER))
    else:
        # Irrelevant stale event inputs cannot leak into the runtime trigger path.
        data[CONF_EVENT_TYPE] = DEFAULT_EVENT_TYPE
        data[CONF_EVENT_AT_FIELD] = DEFAULT_EVENT_AT_FIELD
        data[CONF_EVENT_FILTER] = {}
    if data[CONF_TRIGGER_MODE] == TRIGGER_BINARY_SENSOR:
        sensor = data.get(CONF_BINARY_SENSOR)
        if not isinstance(sensor, str) or not _ENTITY_RE.fullmatch(sensor) or not sensor.startswith("binary_sensor."):
            raise ValidationError(CONF_BINARY_SENSOR, "invalid_entity")
    else:
        data.pop(CONF_BINARY_SENSOR, None)
    return {key: value for key, value in data.items() if key in CONFIG_FIELDS}


def probe_local_endpoints(settings: dict[str, Any], skip_ports: tuple[int, ...] = ()) -> None:
    """Check exact local ownership and availability, without opening a listener."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.bind((settings[CONF_BIND_IP], 0))
    except OSError:
        raise ValidationError(CONF_BIND_IP, "not_local_ip") from None
    for field in (CONF_ONVIF_PORT, CONF_RTSP_PORT):
        if settings[field] in skip_ports:
            continue
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
                probe.bind((settings[CONF_BIND_IP], settings[field]))
        except OSError as error:
            code = "port_in_use" if error.errno == errno.EADDRINUSE else "cannot_bind"
            raise ValidationError(field, code) from None


async def async_validate_settings(hass: Any, raw: dict[str, Any], entry: Any = None) -> dict[str, Any]:
    """Validate entities and all configured endpoints, then probe outside HA's loop."""
    current = entry_settings(entry) if entry is not None else None
    settings = normalize_input(raw, current)
    for field in (CONF_CAMERA_ENTITY, CONF_BINARY_SENSOR):
        if field in settings and hass.states.get(settings[field]) is None:
            raise ValidationError(field, "entity_missing")
    ports = {settings[CONF_ONVIF_PORT], settings[CONF_RTSP_PORT]}
    for other in hass.config_entries.async_entries(DOMAIN):
        if entry is not None and other.entry_id == entry.entry_id:
            continue
        if getattr(other, "source", None) == config_entries.SOURCE_IGNORE:
            continue
        other_settings = entry_settings(other)
        if other_settings.get(CONF_BIND_IP) == settings[CONF_BIND_IP]:
            if ports & {other_settings.get(CONF_ONVIF_PORT), other_settings.get(CONF_RTSP_PORT)}:
                raise ValidationError("base", "entry_port_conflict")

    # The current, successfully loaded entry owns its unchanged listeners.
    skip_ports: tuple[int, ...] = ()
    state = getattr(entry, "state", None)
    if current and getattr(state, "value", state) == "loaded" and current.get(CONF_BIND_IP) == settings[CONF_BIND_IP]:
        skip_ports = tuple(port for port in ports if port in {current.get(CONF_ONVIF_PORT), current.get(CONF_RTSP_PORT)})
    await hass.async_add_executor_job(probe_local_endpoints, settings, skip_ports)
    return settings


def settings_schema(current: dict[str, Any] | None = None, *, editing: bool = False) -> vol.Schema:
    """No existing password or virtual identity is ever put in a UI schema."""
    values = deepcopy(_DEFAULTS) | deepcopy(current or {})
    allowed = values.get(CONF_ALLOWED_CLIENTS, [])
    if isinstance(allowed, list):
        values[CONF_ALLOWED_CLIENTS] = "\n".join(allowed)
    fields: dict[Any, Any] = {}
    fields[vol.Required(CONF_CAMERA_ENTITY, **({"default": values[CONF_CAMERA_ENTITY]} if CONF_CAMERA_ENTITY in values else {}))] = selector.EntitySelector({"filter": {"domain": "camera"}})
    fields[vol.Required(CONF_NAME, default=values[CONF_NAME])] = selector.TextSelector()
    fields[vol.Required(CONF_WINDOW_SECONDS, default=values[CONF_WINDOW_SECONDS])] = selector.NumberSelector({"min": MIN_WINDOW_SECONDS, "max": MAX_WINDOW_SECONDS, "step": 1, "mode": "box", "unit_of_measurement": "s"})
    fields[vol.Required(CONF_BIND_IP, default=values[CONF_BIND_IP])] = selector.TextSelector()
    for key in (CONF_ONVIF_PORT, CONF_RTSP_PORT):
        fields[vol.Required(key, default=values[key])] = selector.NumberSelector({"min": MIN_PORT, "max": MAX_PORT, "step": 1, "mode": "box"})
    fields[vol.Required(CONF_USERNAME, default=values[CONF_USERNAME])] = selector.TextSelector({"autocomplete": "off"})
    password_marker = vol.Optional(CONF_PASSWORD) if editing else vol.Required(CONF_PASSWORD)
    fields[password_marker] = selector.TextSelector({"type": selector.TextSelectorType.PASSWORD, "autocomplete": "new-password"})
    fields[vol.Required(CONF_ALLOWED_CLIENTS, default=values[CONF_ALLOWED_CLIENTS])] = selector.TextSelector({"multiline": True})
    fields[vol.Required(CONF_TRIGGER_MODE, default=values[CONF_TRIGGER_MODE])] = selector.SelectSelector({"options": list(TRIGGER_MODES), "mode": "dropdown", "translation_key": "trigger_mode"})
    fields[vol.Optional(CONF_EVENT_TYPE, default=values[CONF_EVENT_TYPE])] = selector.TextSelector()
    fields[vol.Optional(CONF_EVENT_FILTER, default=values[CONF_EVENT_FILTER])] = selector.ObjectSelector()
    fields[vol.Optional(CONF_EVENT_AT_FIELD, default=values[CONF_EVENT_AT_FIELD])] = selector.TextSelector()
    marker = vol.Optional(CONF_BINARY_SENSOR, **({"default": values[CONF_BINARY_SENSOR]} if values.get(CONF_BINARY_SENSOR) else {}))
    fields[marker] = selector.EntitySelector({"filter": {"domain": "binary_sensor"}})
    return vol.Schema(fields)


def _redisplay_values(value: Any) -> dict[str, Any]:
    """Only known non-secret form fields may be returned after validation fails."""
    if not isinstance(value, dict):
        return {}
    return {key: item for key, item in value.items() if key != CONF_PASSWORD and key in CONFIG_FIELDS}


class HAOnvifExporterConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """One stable virtual ONVIF device for each selected camera entry."""

    VERSION = 1

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: config_entries.ConfigEntry) -> HAOnvifExporterOptionsFlow:
        return HAOnvifExporterOptionsFlow()

    async def _async_initial_defaults(self) -> dict[str, Any]:
        values = deepcopy(_DEFAULTS)
        try:
            values[CONF_BIND_IP] = _ipv4(await network.async_get_source_ip(self.hass), CONF_BIND_IP)
        except (ValidationError, OSError, HomeAssistantError):
            pass
        return values

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> dict[str, Any]:
        errors: dict[str, str] = {}
        defaults = await self._async_initial_defaults()
        if user_input is not None:
            try:
                settings = await async_validate_settings(self.hass, user_input)
            except ValidationError as error:
                errors[error.field] = error.code
                defaults.update(_redisplay_values(user_input))
            else:
                virtual_id = uuid4().hex
                await self.async_set_unique_id(virtual_id)
                return self.async_create_entry(title=settings[CONF_NAME], data=settings | {CONF_VIRTUAL_ID: virtual_id})
        return self.async_show_form(step_id="user", data_schema=settings_schema(defaults), errors=errors)

    async def async_step_reconfigure(self, user_input: dict[str, Any] | None = None) -> dict[str, Any]:
        entry = self._get_reconfigure_entry()
        current = entry_settings(entry)
        errors: dict[str, str] = {}
        if user_input is not None:
            try:
                settings = await async_validate_settings(self.hass, user_input, entry)
            except ValidationError as error:
                errors[error.field] = error.code
                current.update(_redisplay_values(user_input))
            else:
                # Replace settings only; data identity and HA's own entry_id stay fixed.
                data = settings | {CONF_VIRTUAL_ID: entry.data[CONF_VIRTUAL_ID]}
                return self.async_update_reload_and_abort(entry, title=settings[CONF_NAME], data=data, options={})
        return self.async_show_form(step_id="reconfigure", data_schema=settings_schema(current, editing=True), errors=errors)


class HAOnvifExporterOptionsFlow(config_entries.OptionsFlowWithReload):
    """Edit every setting and reload only this exporter entry."""

    async def async_step_init(self, user_input: dict[str, Any] | None = None) -> dict[str, Any]:
        current = entry_settings(self.config_entry)
        errors: dict[str, str] = {}
        if user_input is not None:
            try:
                settings = await async_validate_settings(self.hass, user_input, self.config_entry)
            except ValidationError as error:
                errors[error.field] = error.code
                current.update(_redisplay_values(user_input))
            else:
                return self.async_create_entry(title="", data=settings)
        return self.async_show_form(step_id="init", data_schema=settings_schema(current, editing=True), errors=errors)
