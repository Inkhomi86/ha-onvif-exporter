"""Hermetic HA diagnostics contract tests, without HA/config/storage/HTTP access."""
from __future__ import annotations

import asyncio
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
import unittest

BASE = Path(__file__).resolve().parents[1] / "custom_components" / "ha_onvif_exporter"
spec = importlib.util.spec_from_file_location("exporter_diagnostics_fixture", BASE / "diagnostics.py")
diagnostics = importlib.util.module_from_spec(spec)
spec.loader.exec_module(diagnostics)


class Entry:
    def __init__(self, manager):
        self.runtime_data = manager

    def __getattr__(self, name):
        raise AssertionError("Diagnostics must not read entry configuration or IDs")


class Hass:
    def __getattribute__(self, name):
        raise AssertionError("Diagnostics must not read HA registries/storage or perform I/O")


class Onvif:
    def __init__(self):
        self.loop = asyncio.get_running_loop()
        self.calls = 0

    def diagnostics_snapshot(self):
        if asyncio.get_running_loop() is not self.loop:
            raise RuntimeError("wrong loop")
        self.calls += 1
        return {"is_motion": True, "active_subscriptions": 1,
                "pulling_subscriptions": 1, "queued_notifications": 0,
                "inflight_requests": 1, "operations_successful": {"events_pull_messages": 2},
                "pull_notifications_true": 1, "pull_notifications_false": 1}


class DiagnosticsTests(unittest.IsolatedAsyncioTestCase):
    async def test_official_async_handler_has_no_ha_executor_or_configuration_reads(self):
        self.assertTrue(asyncio.iscoroutinefunction(diagnostics.async_get_config_entry_diagnostics))
        onvif = Onvif()
        manager = SimpleNamespace(status="live", motion=True, engine=SimpleNamespace(frames=45), onvif=onvif)
        entry = Entry(manager)
        result = await diagnostics.async_get_config_entry_diagnostics(Hass(), entry)
        self.assertEqual(result["manager"], {"status": "live", "is_motion": True, "exported_frames": 45})
        self.assertEqual(result["onvif"]["active_subscriptions"], 1)
        self.assertEqual(onvif.calls, 1)
        self.assertIs(entry.runtime_data, manager)
        self.assertEqual(set(result), {"loaded", "manager", "onvif"})

    async def test_unloaded_entry_returns_only_available_boolean(self):
        self.assertEqual(await diagnostics.async_get_config_entry_diagnostics(Hass(), Entry(None)), {"loaded": False})

    async def test_status_and_frame_values_are_allowlisted_not_arbitrary_errors_or_secrets(self):
        forbidden = "PRIVATE_DIAGNOSTIC_SECRET"
        for frames in (-1, True, "PRIVATE_DIAGNOSTIC_SECRET", 1 << 54):
            manager = SimpleNamespace(status=forbidden, motion=forbidden,
                                      engine=SimpleNamespace(frames=frames), onvif=Onvif())
            result = await diagnostics.async_get_config_entry_diagnostics(Hass(), Entry(manager))
            self.assertEqual(result["manager"], {"status": "unavailable", "is_motion": False, "exported_frames": 0})
            text = json.dumps(result)
            for value in (forbidden, "password", "options", "entry_id", "client", "rtsp", "http", "subscription_id"):
                self.assertNotIn(value, text)


if __name__ == "__main__":
    unittest.main()
