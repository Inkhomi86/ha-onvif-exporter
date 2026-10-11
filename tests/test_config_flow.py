"""Local UI/validation tests; no Home Assistant, listeners or camera is contacted.

Uses the real voluptuous 0.15.2 pinned by HA 2026.8.1 and small HA UI fakes.
Run with Python and PYTHONPATH pointing at ../.test-deps.
"""

from __future__ import annotations

import asyncio
import errno
import importlib.util
import json
import socket
import sys
import types
import unittest
from copy import deepcopy
from pathlib import Path
from unittest.mock import AsyncMock, patch

import voluptuous as vol

ROOT = Path(__file__).resolve().parents[1]
COMPONENT = ROOT / "custom_components" / "ha_onvif_exporter"


class FakeFlow:
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__()

    def __init__(self):
        self.hass = None
        self.unique_id = None
        self.context = {}

    async def async_set_unique_id(self, unique_id):
        self.unique_id = unique_id

    def async_create_entry(self, *, title, data):
        return {"type": "create_entry", "title": title, "data": data}

    def async_show_form(self, **kwargs):
        return {"type": "form", **kwargs}

    def _get_reconfigure_entry(self):
        return self.hass.config_entries.async_get_known_entry(self.context["entry_id"])

    def async_update_reload_and_abort(self, entry, **kwargs):
        entry.data = kwargs["data"]
        entry.options = kwargs["options"]
        entry.title = kwargs["title"]
        self.hass.config_entries.reloads.append(entry.entry_id)
        return {"type": "abort", "reason": "reconfigure_successful"}


class FakeOptionsFlow(FakeFlow):
    automatic_reload = True

    @property
    def config_entry(self):
        return self.hass.config_entries.async_get_known_entry(self.handler)


class FakeSelector:
    def __init__(self, config=None):
        self.config = config or {}

    def __call__(self, value):
        return value


def load_flow():
    """Isolate this import from peer tests and never import integration __init__."""
    package = types.ModuleType("_ha_onvif_exporter_ui_review")
    package.__path__ = [str(COMPONENT)]
    homeassistant = types.ModuleType("homeassistant")
    entries = types.ModuleType("homeassistant.config_entries")
    entries.ConfigFlow = FakeFlow
    entries.ConfigEntry = object
    entries.OptionsFlowWithReload = FakeOptionsFlow
    entries.SOURCE_IGNORE = "ignore"
    homeassistant.config_entries = entries
    components = types.ModuleType("homeassistant.components")
    network = types.ModuleType("homeassistant.components.network")
    network.async_get_source_ip = AsyncMock(return_value="10.9.8.7")
    components.network = network
    core = types.ModuleType("homeassistant.core")
    core.callback = lambda callback: callback
    exceptions = types.ModuleType("homeassistant.exceptions")
    exceptions.HomeAssistantError = type("HomeAssistantError", (Exception,), {})
    helpers = types.ModuleType("homeassistant.helpers")
    selectors = types.ModuleType("homeassistant.helpers.selector")
    for name in ("EntitySelector", "TextSelector", "NumberSelector", "SelectSelector", "ObjectSelector"):
        setattr(selectors, name, type(name, (FakeSelector,), {}))
    selectors.TextSelectorType = types.SimpleNamespace(PASSWORD="password")
    helpers.selector = selectors
    modules = {
        "_ha_onvif_exporter_ui_review": package,
        "homeassistant": homeassistant,
        "homeassistant.config_entries": entries,
        "homeassistant.components": components,
        "homeassistant.components.network": network,
        "homeassistant.core": core,
        "homeassistant.exceptions": exceptions,
        "homeassistant.helpers": helpers,
        "homeassistant.helpers.selector": selectors,
    }
    with patch.dict(sys.modules, modules):
        spec = importlib.util.spec_from_file_location(
            "_ha_onvif_exporter_ui_review.config_flow", COMPONENT / "config_flow.py"
        )
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
    return module


FLOW = load_flow()
FIXTURE_PASSWORD = "ui-test-password-only"


def valid(**updates):
    return {
        "camera_entity": "camera.front_gate",
        "name": "Front Gate virtual",
        "window_seconds": 30,
        "bind_ip": "10.9.8.7",
        "onvif_port": 1986,
        "rtsp_port": 18557,
        "username": "ui_test",
        "password": FIXTURE_PASSWORD,
        "allowed_clients": "10.9.8.1\n10.9.7.56",
        "trigger_mode": "manual",
        **updates,
    }


class FakeEntry:
    def __init__(self, entry_id="entry-a", settings=None, *, state="loaded", options=None):
        self.entry_id = entry_id
        self.data = FLOW.normalize_input(settings or valid()) | {"virtual_id": "b" * 32}
        self.options = options or {}
        self.state = state
        self.source = "user"
        self.title = self.data["name"]
        self.unique_id = self.data["virtual_id"]


class FakeHass:
    def __init__(self, entries=(), entities=("camera.front_gate", "binary_sensor.door_ring")):
        self.entities = set(entities)
        self.states = types.SimpleNamespace(get=lambda entity: object() if entity in self.entities else None)
        self._entries = {entry.entry_id: entry for entry in entries}
        self.config_entries = types.SimpleNamespace(
            async_entries=lambda domain: list(self._entries.values()),
            async_get_known_entry=lambda entry_id: self._entries[entry_id],
            reloads=[],
        )
        self.executor_calls = []

    async def async_add_executor_job(self, function, *args):
        self.executor_calls.append((function, args))
        return function(*args)


class NormalizeTests(unittest.TestCase):
    def rejected(self, field, code, **change):
        with self.assertRaises(FLOW.ValidationError) as result:
            FLOW.normalize_input(valid(**change))
        self.assertEqual((result.exception.field, result.exception.code), (field, code))

    def test_defaults_are_generic_fail_closed_no_private_entry(self):
        self.assertEqual(FLOW._DEFAULTS["trigger_mode"], "manual")
        self.assertEqual(FLOW._DEFAULTS["allowed_clients"], [])
        self.assertEqual(FLOW._DEFAULTS["event_filter"], {})
        self.assertNotIn("password", FLOW._DEFAULTS)

    def test_valid_preserves_explicit_settings_and_normalizes_clients(self):
        result = FLOW.normalize_input(valid(allowed_clients="10.9.8.1\n10.9.8.1\n 10.9.7.56 ", window_seconds=30.0))
        self.assertEqual(result["allowed_clients"], ["10.9.8.1", "10.9.7.56"])
        self.assertEqual(result["window_seconds"], 30)
        self.assertEqual(type(result["window_seconds"]), int)
        self.assertNotIn("virtual_id", result)
        self.assertNotIn("binary_sensor", result)

    def test_no_unrecognized_config_or_identity_injection(self):
        for key in ("virtual_id", "agent_token", "audio_enabled", "activate_action"):
            with self.subTest(key=key):
                self.rejected("base", "invalid_input", **{key: "not-allowed"})

    def test_invalid_bind_and_allowlist_ranges(self):
        for ip in ("0.0.0.0", "::1", "10.9.8.0/24", "239.0.0.1", "255.255.255.255", "*", " 10.9.8.7"):
            with self.subTest(ip=ip):
                self.rejected("bind_ip", "invalid_ip", bind_ip=ip)
        for peers in ("", [], "10.9.8.0/24", "*", "0.0.0.0", "1.2.3.4,5.6.7.8", ["10.9.8.1"] * 33, " " * 1025):
            with self.subTest(peers=peers):
                self.rejected("allowed_clients", "invalid_clients", allowed_clients=peers)

    def test_bounds_and_non_finite_numbers(self):
        for number in (True, 0, 301, 30.1, float("nan"), float("inf"), 2**10000, "30"):
            with self.subTest(number=type(number).__name__):
                self.rejected("window_seconds", "invalid_number", window_seconds=number)
        self.rejected("onvif_port", "invalid_number", onvif_port=80)
        self.rejected("rtsp_port", "invalid_number", rtsp_port=65536)
        self.rejected("rtsp_port", "same_port", rtsp_port=1986)

    def test_password_and_username_validation(self):
        for password in ("", "short", "a" * 15, "é" * 16, "a" * 129, "a\n" * 8, "a\x7f" * 8, None):
            with self.subTest(password_type=type(password).__name__):
                self.rejected("password", "invalid_password", password=password)
        for username in ("user:password", "user name", "é", "a" * 65, "any", "window_publisher", None):
            self.rejected("username", "invalid_username", username=username)

    def test_camera_and_binary_sensor_domains(self):
        self.rejected("camera_entity", "invalid_entity", camera_entity="button.unlock")
        self.rejected("binary_sensor", "invalid_entity", trigger_mode="binary_sensor", binary_sensor="camera.front_gate")
        self.rejected("binary_sensor", "invalid_entity", trigger_mode="binary_sensor")
        result = FLOW.normalize_input(valid(trigger_mode="binary_sensor", binary_sensor="binary_sensor.door_ring"))
        self.assertEqual(result["binary_sensor"], "binary_sensor.door_ring")

    def test_flat_filter_retains_scalar_and_or_values_without_aliasing(self):
        source = {"entry_id": "opaque-entry", "event_key": ["doorbell_pressed", "door_unlock_started"], "enabled": True, "optional": None, "id": 23}
        result = FLOW.normalize_input(valid(trigger_mode="events", event_filter=source))
        self.assertEqual(result["event_filter"], source)
        source["event_key"].append("unapproved")
        self.assertNotIn("unapproved", result["event_filter"]["event_key"])

    def test_invalid_event_schema_is_rejected(self):
        self.rejected("event_type", "invalid_event", trigger_mode="events", event_type="bad event")
        self.rejected("event_at_field", "invalid_event", trigger_mode="events", event_at_field="nested.timestamp")
        for filt in ([], {"nested": {"key": "v"}}, {"key": []}, {"key": ["x"] * 17}, {"key": 1.0}, {"key": "x" * 257}, {"key": "\ud800"}, {"key": "a\n"}, {"a.b": "x"}, {"key": 2**53}, {f"key{i}": "v" for i in range(17)}):
            with self.subTest(filter_type=type(filt).__name__):
                self.rejected("event_filter", "invalid_filter", trigger_mode="events", event_filter=filt)

    def test_edited_blank_password_is_preserved_but_initial_blank_denied(self):
        current = FLOW.normalize_input(valid())
        for raw in ({"name": "Renamed"}, {"password": ""}):
            result = FLOW.normalize_input(raw, current)
            self.assertEqual(result["password"], FIXTURE_PASSWORD)
        result = FLOW.normalize_input({"password": "rotated-test-secret"}, current)
        self.assertEqual(result["password"], "rotated-test-secret")
        self.rejected("password", "invalid_password", password="")

    def test_safe_name_and_unknown_trigger(self):
        for name in (" ", "x" * 101, "x\n", "\ud800", None):
            self.rejected("name", "invalid_text", name=name)
        self.rejected("trigger_mode", "invalid_trigger", trigger_mode="always_open")

    def test_manual_ignores_inactive_sensor_and_filter_inputs(self):
        result = FLOW.normalize_input(valid(binary_sensor="button.fake", event_filter={"nested": {"bad": "v"}}))
        self.assertNotIn("binary_sensor", result)
        self.assertEqual(result["event_filter"], {})


class SchemaTests(unittest.TestCase):
    def test_no_existing_or_failed_password_default_in_any_form(self):
        for editing in (False, True):
            schema = FLOW.settings_schema(valid(), editing=editing)
            markers = {str(marker): marker for marker in schema.schema}
            password = markers["password"]
            self.assertIs(password.default, vol.UNDEFINED)
            self.assertNotIn("virtual_id", markers)
            self.assertNotIn("audio_enabled", markers)
            selector = schema.schema[password]
            self.assertEqual(selector.config["type"], "password")
            self.assertEqual(selector.config["autocomplete"], "new-password")
        self.assertIsInstance(password, vol.Optional)

    def test_schema_all_settings_and_domain_filters(self):
        schema = FLOW.settings_schema()
        markers = {str(marker): marker for marker in schema.schema}
        self.assertEqual(set(markers), FLOW.CONFIG_FIELDS)
        self.assertEqual(schema.schema[markers["camera_entity"]].config["filter"], {"domain": "camera"})
        self.assertEqual(schema.schema[markers["binary_sensor"]].config["filter"], {"domain": "binary_sensor"})

    def test_metadata_translations_cover_fields_and_error_codes(self):
        reference = json.loads((COMPONENT / "strings.json").read_text(encoding="utf-8"))
        for locale in ("en", "es"):
            translation = json.loads((COMPONENT / "translations" / f"{locale}.json").read_text(encoding="utf-8"))
            for mode, step in (("config", "user"), ("config", "reconfigure"), ("options", "init")):
                self.assertEqual(set(translation[mode]["step"][step]["data"]), FLOW.CONFIG_FIELDS)
            for mode in ("config", "options"):
                self.assertEqual(set(translation[mode]["error"]), set(reference[mode]["error"]))
            self.assertEqual(set(translation["selector"]["trigger_mode"]["options"]), set(FLOW.TRIGGER_MODES))
        self.assertEqual(json.loads((COMPONENT / "translations" / "en.json").read_text(encoding="utf-8")), reference)
        manifest = json.loads((COMPONENT / "manifest.json").read_text(encoding="utf-8"))
        self.assertTrue(manifest["config_flow"])
        self.assertEqual(manifest["requirements"], ["aiortc==1.15.0"])
        self.assertEqual(manifest["domain"], FLOW.DOMAIN)
        self.assertEqual(manifest["documentation"], "https://github.com/Inkhomi86/ha-onvif-exporter")
        self.assertEqual(json.loads((ROOT / "hacs.json").read_text(encoding="utf-8"))["homeassistant"], "2026.8.1")


class FakeSocket:
    def __init__(self, factory):
        self.factory = factory
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.closed = True

    def bind(self, endpoint):
        self.factory.binds.append(endpoint)
        if endpoint in self.factory.errors:
            raise self.factory.errors[endpoint]


class SocketFactory:
    def __init__(self, errors=None):
        self.binds = []
        self.sockets = []
        self.errors = errors or {}

    def __call__(self, family, socket_type):
        if family != socket.AF_INET or socket_type != socket.SOCK_STREAM:
            raise AssertionError("only IPv4 TCP probing is allowed")
        obj = FakeSocket(self)
        self.sockets.append(obj)
        return obj


class ValidationAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_exact_local_bind_then_two_unopened_closed_socket_probes(self):
        factory = SocketFactory()
        hass = FakeHass()
        with patch.object(FLOW.socket, "socket", factory):
            result = await FLOW.async_validate_settings(hass, valid())
        self.assertEqual(factory.binds, [("10.9.8.7", 0), ("10.9.8.7", 1986), ("10.9.8.7", 18557)])
        self.assertTrue(all(sock.closed for sock in factory.sockets))
        self.assertEqual(len(hass.executor_calls), 1)
        self.assertEqual(result["rtsp_port"], 18557)

    async def test_missing_entity_no_socket_opened(self):
        hass = FakeHass(entities=())
        factory = SocketFactory()
        with patch.object(FLOW.socket, "socket", factory):
            with self.assertRaises(FLOW.ValidationError) as result:
                await FLOW.async_validate_settings(hass, valid())
        self.assertEqual(result.exception.code, "entity_missing")
        self.assertEqual(factory.binds, [])

    async def test_other_entry_cross_protocol_conflict_is_rejected(self):
        other = FakeEntry("other", valid(onvif_port=2055, rtsp_port=1986))
        hass = FakeHass((other,))
        with self.assertRaises(FLOW.ValidationError) as result:
            await FLOW.async_validate_settings(hass, valid())
        self.assertEqual(result.exception.code, "entry_port_conflict")
        self.assertEqual(hass.executor_calls, [])

    async def test_other_address_and_ignore_entry_are_not_false_conflicts(self):
        other = FakeEntry("other", valid(bind_ip="10.9.8.8"))
        ignored = FakeEntry("ignored")
        ignored.source = "ignore"
        factory = SocketFactory()
        with patch.object(FLOW.socket, "socket", factory):
            await FLOW.async_validate_settings(FakeHass((other, ignored)), valid())
        self.assertEqual(len(factory.binds), 3)

    async def test_loaded_entry_unchanged_ports_are_owned_but_changes_are_probed(self):
        entry = FakeEntry()
        factory = SocketFactory()
        with patch.object(FLOW.socket, "socket", factory):
            await FLOW.async_validate_settings(FakeHass((entry,)), {"name": "Changed"}, entry)
            await FLOW.async_validate_settings(FakeHass((entry,)), {"rtsp_port": 18558}, entry)
        self.assertEqual(factory.binds, [("10.9.8.7", 0), ("10.9.8.7", 0), ("10.9.8.7", 18558)])

    async def test_unloaded_entry_does_not_skip_occupied_old_port(self):
        entry = FakeEntry(state="not_loaded")
        factory = SocketFactory({("10.9.8.7", 1986): OSError(errno.EADDRINUSE, "fixture only")})
        with patch.object(FLOW.socket, "socket", factory):
            with self.assertRaises(FLOW.ValidationError) as result:
                await FLOW.async_validate_settings(FakeHass((entry,)), {}, entry)
        self.assertEqual(result.exception.code, "port_in_use")

    async def test_socket_errors_only_safe_codes_no_raw_error(self):
        for endpoint, code in ((("10.9.8.7", 0), "not_local_ip"), (("10.9.8.7", 18557), "cannot_bind")):
            factory = SocketFactory({endpoint: OSError(errno.EACCES, FIXTURE_PASSWORD)})
            with patch.object(FLOW.socket, "socket", factory):
                with self.assertRaises(FLOW.ValidationError) as result:
                    await FLOW.async_validate_settings(FakeHass(), valid())
            self.assertEqual(str(result.exception), code)
            self.assertNotIn(FIXTURE_PASSWORD, str(result.exception))


class FlowTests(unittest.IsolatedAsyncioTestCase):
    async def test_user_creation_stable_uuid_and_local_autodetection(self):
        flow = FLOW.HAOnvifExporterConfigFlow()
        flow.hass = FakeHass()
        first = await flow.async_step_user()
        marker = next(key for key in first["data_schema"].schema if str(key) == "bind_ip")
        self.assertEqual(marker.default(), "10.9.8.7")
        with patch.object(FLOW, "probe_local_endpoints", return_value=None):
            created = await flow.async_step_user(valid())
        self.assertEqual(created["type"], "create_entry")
        self.assertEqual(created["data"]["virtual_id"], flow.unique_id)
        self.assertRegex(flow.unique_id, r"^[0-9a-f]{32}$")

    async def test_network_failure_safe_loopback_default(self):
        flow = FLOW.HAOnvifExporterConfigFlow()
        flow.hass = FakeHass()
        with patch.object(FLOW.network, "async_get_source_ip", AsyncMock(side_effect=FLOW.HomeAssistantError("fixture"))):
            values = await flow._async_initial_defaults()
        self.assertEqual(values["bind_ip"], "127.0.0.1")

    async def test_user_error_does_not_redisplay_attempted_password(self):
        flow = FLOW.HAOnvifExporterConfigFlow()
        flow.hass = FakeHass()
        result = await flow.async_step_user(valid(allowed_clients="*"))
        self.assertEqual(result["errors"], {"allowed_clients": "invalid_clients"})
        marker = next(key for key in result["data_schema"].schema if str(key) == "password")
        self.assertIs(marker.default, vol.UNDEFINED)

    async def test_malformed_flow_input_returns_safe_error_without_redisplay(self):
        flow = FLOW.HAOnvifExporterConfigFlow()
        flow.hass = FakeHass()
        result = await flow.async_step_user([FIXTURE_PASSWORD])
        self.assertEqual(result["errors"], {"base": "invalid_input"})
        marker = next(key for key in result["data_schema"].schema if str(key) == "password")
        self.assertIs(marker.default, vol.UNDEFINED)

    async def test_options_changes_all_settings_preserves_identity_and_secret(self):
        entry = FakeEntry()
        before = deepcopy(entry.data)
        flow = FLOW.HAOnvifExporterConfigFlow.async_get_options_flow(entry)
        flow.hass = FakeHass((entry,))
        flow.handler = entry.entry_id
        with patch.object(FLOW, "probe_local_endpoints", return_value=None):
            result = await flow.async_step_init({"window_seconds": 45, "name": "New name", "password": ""})
        self.assertEqual(result["data"]["password"], FIXTURE_PASSWORD)
        self.assertEqual(result["data"]["window_seconds"], 45)
        self.assertNotIn("virtual_id", result["data"])
        self.assertEqual(entry.data, before)
        self.assertTrue(flow.automatic_reload)

    async def test_reconfigure_merges_options_but_never_changes_uuid(self):
        entry = FakeEntry(options={"window_seconds": 45})
        before_id = entry.data["virtual_id"]
        flow = FLOW.HAOnvifExporterConfigFlow()
        flow.hass = FakeHass((entry,))
        flow.context = {"entry_id": entry.entry_id}
        with patch.object(FLOW, "probe_local_endpoints", return_value=None):
            result = await flow.async_step_reconfigure({"name": "Renamed", "password": ""})
        self.assertEqual(result["type"], "abort")
        self.assertEqual(entry.data["virtual_id"], before_id)
        self.assertEqual(entry.unique_id, before_id)
        self.assertEqual(entry.data["window_seconds"], 45)
        self.assertEqual(entry.data["password"], FIXTURE_PASSWORD)
        self.assertEqual(entry.options, {})
        self.assertEqual(flow.hass.config_entries.reloads, [entry.entry_id])


if __name__ == "__main__":
    unittest.main()
