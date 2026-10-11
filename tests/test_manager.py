"""Independent hermetic manager/policy review. No HA, sockets, or devices."""
import asyncio
import importlib.util
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
import unittest
from unittest import mock

BASE = Path(__file__).resolve().parents[1] / "custom_components" / "ha_onvif_exporter"
PACKAGE = "exporter_manager_review_fixture"
package = ModuleType(PACKAGE)
package.__path__ = [str(BASE)]
sys.modules[PACKAGE] = package

def load(name):
    spec = importlib.util.spec_from_file_location(PACKAGE + "." + name, BASE / (name + ".py"))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module

policy = load("policy")
manager_module = load("manager")
engine_module = load("engine")


class Clock:
    def __init__(self, now=100.0): self.now = now
    def __call__(self): return self.now


class Bus:
    def __init__(self): self.listeners = {}; self.unsubscribed = 0
    def async_listen(self, kind, callback):
        self.listeners[kind] = callback
        def remove():
            self.listeners.pop(kind, None)
            self.unsubscribed += 1
        return remove
    def emit(self, kind, data): self.listeners[kind](SimpleNamespace(data=data))


class Hass:
    def __init__(self): self.bus = Bus()
    def async_create_task(self, coroutine, *, eager_start=False, name=None):
        assert eager_start is False
        return asyncio.create_task(coroutine, name=name)


class Consumer:
    def __init__(self, trace):
        self.trace = trace
        self.session_id = None
        self.cleanup_ok = True
        self.active = False
        self.frame_ready = False
        self.failure_code = None
        self.open_calls = self.close_calls = 0
        self.open_gate = asyncio.Event(); self.open_gate.set()
        self.frame_gate = asyncio.Event(); self.frame_gate.set()
        self.close_gate = asyncio.Event(); self.close_gate.set()
        self.failed = asyncio.Event()
        self.fail_frame = False
        self.closed = asyncio.Event()
        self.source = "rtsp://127.0.0.1:18554/camera_fixture"
    async def begin(self, entity_id):
        self.open_calls += 1
        self.trace.append("consumer_open")
        self.session_id = str(self.open_calls)
        self.closed = asyncio.Event()
        self.failed = asyncio.Event()
        await self.open_gate.wait()
        self.active = True
        self.frame_ready = False
        return SimpleNamespace(source=self.source, session_id=self.session_id)
    async def wait_frame(self):
        self.trace.append("frame_wait")
        try:
            await self.frame_gate.wait()
            if self.fail_frame: raise RuntimeError("PRIVATE_FIXTURE_FRAME_FAILURE")
            self.frame_ready = True
            self.trace.append("frame_ready")
        finally:
            self.trace.append("frame_wait_done")
    async def open(self, entity_id):
        info = await self.begin(entity_id)
        await self.wait_frame()
        return info
    async def close(self):
        self.close_calls += 1
        self.trace.append("consumer_close")
        await self.close_gate.wait()
        self.active = False
        self.frame_ready = False
        self.closed.set()
        self.failed.set()
    async def wait_closed(self):
        reference = self.closed
        await reference.wait()
    async def wait_failure(self):
        reference = self.failed
        await reference.wait()
        return "media_failed"


class Engine:
    def __init__(self, trace):
        self.trace = trace
        self.callback = None
        self.video = False
        self.started = False
        self.deadlines = []
        self.cleanup_confirmed = True
        self.start_gate = asyncio.Event(); self.start_gate.set()
        self.fail_start_video = False
        self.fail_once = False
        self.video_stops = 0
        self.cancel_gate = asyncio.Event(); self.cancel_gate.set()
    @property
    def video_running(self): return self.video
    def set_callback(self, callback): self.callback = callback
    async def start(self): self.trace.append("engine_start"); self.started = True
    async def stop(self): self.trace.append("engine_stop"); self.started = False; return self.cleanup_confirmed
    def extend_deadline(self, deadline): self.deadlines.append(deadline)
    async def start_video(self, source):
        self.trace.append("video_start")
        try:
            await self.start_gate.wait()
            if self.fail_start_video:
                if self.fail_once: self.fail_start_video = False
                raise RuntimeError("PUBLIC_FAKE_FAILURE")
            self.video = True
        except asyncio.CancelledError:
            await self.cancel_gate.wait()
            self.trace.append("video_cancel_joined")
            raise
    async def stop_video(self):
        self.trace.append("video_stop")
        self.video_stops += 1
        if self.cleanup_confirmed: self.video = False
        return self.cleanup_confirmed


class Onvif:
    def __init__(self, trace): self.trace = trace; self.motion = False; self.fail_start = False
    async def start(self):
        self.trace.append("onvif_start")
        if self.fail_start: raise RuntimeError("PUBLIC_FAKE_FAILURE")
    async def stop(self): self.trace.append("onvif_stop")
    def set_motion(self, state): self.motion = state; self.trace.append("motion_" + str(state))


class ManagerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.trace = []
        self.hass = Hass()
        self.consumer, self.engine, self.onvif = Consumer(self.trace), Engine(self.trace), Onvif(self.trace)
        self.settings = {
            "camera_entity": "camera.fixture", "window_seconds": 30,
            "event_filter": {"entry_id": "selected", "event_key": ["doorbell_pressed", "door_unlock_started"]},
            "event_at_field": "event_at", "event_type": "fixture_event", "trigger_mode": "manual",
        }
        self.manager = manager_module.ExportManager(self.hass, "export-entry", self.settings,
                                                    self.consumer, self.engine, self.onvif)
        self.clock = Clock()
        # Replace only this module's binding, not Python's shared time module
        # (asyncio itself must retain its real monotonic clock).
        self.monotonic = mock.patch.object(manager_module, "time", SimpleNamespace(monotonic=self.clock))
        self.monotonic.start()

    async def asyncTearDown(self):
        self.manager._listeners.clear()
        for name in ("open_gate", "frame_gate", "close_gate"):
            gate = getattr(self.consumer, name, None)
            if gate is not None: gate.set()
        self.engine.start_gate.set(); self.engine.cancel_gate.set()
        self.engine.cleanup_confirmed = True
        await self.manager.stop()
        self.monotonic.stop()

    async def settle(self, turns=30):
        for _ in range(turns): await asyncio.sleep(0)

    async def live(self):
        await self.manager.start()
        self.assertTrue(await self.manager.async_activate())
        await self.settle()
        self.assertEqual(self.manager.status, "live")

    async def test_start_is_standby_without_camera_activation(self):
        await self.manager.start()
        self.assertEqual(self.consumer.open_calls, 0)
        self.assertFalse(self.engine.video)
        self.assertFalse(self.manager.motion)

    async def test_manual_activation_opens_one_own_consumer_then_video(self):
        await self.live()
        self.assertLess(self.trace.index("consumer_open"), self.trace.index("video_start"))
        self.assertTrue(self.onvif.motion)

    async def test_latest_activation_renews_30_seconds_not_first(self):
        await self.live()
        self.assertEqual(self.manager.deadline, 130)
        self.clock.now = 125
        await self.manager.async_activate()
        await self.settle()
        self.assertEqual(self.manager.deadline, 155)
        self.assertEqual(self.consumer.open_calls, 1)

    async def test_deactivate_reaps_worker_before_closing_reference(self):
        await self.live()
        await self.manager.async_deactivate()
        self.assertLess(self.trace.index("video_stop"), self.trace.index("consumer_close"))
        self.assertFalse(self.manager.motion)
        self.assertFalse(self.engine.video)

    async def test_expired_window_closes_owned_consumer(self):
        await self.live()
        self.clock.now = 131
        self.manager._expired()
        await self.settle()
        self.assertFalse(self.manager.motion)
        self.assertFalse(self.consumer.active)
        self.assertEqual(self.manager.status, "standby")

    async def test_expiry_scheduled_before_renewal_must_not_close_fresh_window(self):
        await self.live()
        self.clock.now = 131
        self.manager._expired()  # queued expiry has not run yet
        await self.manager.async_activate()  # new authorized activation
        await self.settle(20)
        self.assertEqual(self.manager.deadline, 161)
        self.assertEqual(self.engine.video_stops, 0,
                         "A stale expiry closed an already renewed live window")
        self.assertEqual(self.consumer.open_calls, 1)

    async def test_listener_error_cannot_block_deactivate_cleanup(self):
        await self.live()
        def broken_listener(): raise RuntimeError("PUBLIC_FAKE_LISTENER_FAILURE")
        self.manager.listen(broken_listener)
        await self.manager.async_deactivate()
        self.assertFalse(self.engine.video)
        self.assertFalse(self.consumer.active)

    async def test_engine_expiry_queued_before_renewal_reopens_publisher_not_consumer(self):
        await self.live()
        self.clock.now = 131
        # The real watchdog kills its publisher before delivering this notice.
        self.engine.video = False
        self.engine.callback("expired")
        await self.manager.async_activate()
        await self.settle(20)
        self.assertEqual(self.engine.video_stops, 1)
        self.assertTrue(self.engine.video)
        self.assertEqual(self.consumer.open_calls, 1)
        self.assertEqual(self.consumer.close_calls, 0,
                         "Publisher recovery must retain the valid HA consumer")

    async def test_delayed_event_task_uses_event_deadline_not_execution_time(self):
        self.settings["trigger_mode"] = "events"
        self.manager._events = policy.EventAdmission(self.settings["event_filter"], "event_at", clock=self.clock)
        await self.manager.start()
        self.clock.now = 101
        self.hass.bus.emit("fixture_event", {"entry_id": "selected", "event_key": "door_unlock_started", "event_at": 101})
        # Dispatcher queued a coroutine; execution is delayed, not a new event.
        self.clock.now = 111
        await self.settle()
        self.assertEqual(self.manager.deadline, 131,
                         "A delayed task must not add its scheduling delay to the window")

    async def test_stale_event_age_does_not_shorten_newer_activation(self):
        await self.live()
        self.clock.now = 105
        await self.manager.async_activate(activation_at=self.clock.now - 8)
        self.assertEqual(self.manager.deadline, 130)

    async def test_disabled_export_does_not_activate_camera(self):
        await self.manager.start()
        await self.manager.async_set_enabled(False)
        self.assertFalse(await self.manager.async_activate())
        self.assertEqual(self.consumer.open_calls, 0)

    async def test_shutdown_cancels_pending_open_without_starting_video(self):
        await self.manager.start()
        self.consumer.open_gate.clear()
        await self.manager.async_activate()
        await self.settle(2)
        await self.manager.stop()
        self.assertNotIn("video_start", self.trace)
        self.assertFalse(self.consumer.active)
        self.assertFalse(self.manager.motion)

    async def test_stop_during_publisher_start_cleans_reference(self):
        await self.manager.start()
        self.engine.start_gate.clear()
        await self.manager.async_activate()
        await self.settle(4)
        await self.manager.stop()
        self.assertFalse(self.consumer.active)
        self.assertFalse(self.manager.motion)

    async def test_unconfirmed_publisher_reap_keeps_reference_and_poisoned(self):
        await self.live()
        self.engine.cleanup_confirmed = False
        await self.manager.async_deactivate()
        self.assertTrue(self.consumer.active)
        self.assertTrue(self.manager._poisoned)
        self.assertFalse(await self.manager.async_activate())
        self.assertEqual(self.manager.error_code, "publisher_cleanup_unconfirmed")

    async def test_unconfirmed_consumer_cleanup_cannot_reopen(self):
        await self.live()
        self.consumer.cleanup_ok = False
        await self.manager.async_deactivate()
        self.assertTrue(self.manager._poisoned)
        self.assertFalse(await self.manager.async_activate())
        self.assertEqual(self.consumer.open_calls, 1)

    async def test_video_failure_closes_scoped_reference(self):
        self.engine.fail_start_video = True
        await self.manager.start()
        await self.manager.async_activate()
        await self.settle()
        self.assertEqual(self.manager.status, "fault")
        self.assertFalse(self.consumer.active)

    async def test_worker_diagnostic_is_safe_and_survives_cleanup(self):
        self.engine.fail_start_video = True
        self.engine.failure_code = "media_pipeline_source_open"
        await self.manager.start()
        await self.manager.async_activate()
        await self.settle()
        self.assertEqual(self.manager.error_code, "media_pipeline_source_open")
        self.assertFalse(self.consumer.active)

    async def test_worker_unknown_diagnostic_is_not_exposed(self):
        self.engine.fail_start_video = True
        self.engine.failure_code = "not-a-public-code"
        await self.manager.start()
        await self.manager.async_activate()
        await self.settle()
        self.assertEqual(self.manager.error_code, "activation_failed")
        self.assertEqual(self.manager.status, "fault")
        self.assertFalse(self.consumer.active)

    async def test_worker_fault_cannot_hide_cleanup_failure(self):
        await self.live()
        self.engine.cleanup_confirmed = False
        self.engine.failure_code = "media_pipeline_decode_filter_mux"
        await self.manager._finish("worker_failed", clear=True)
        self.assertEqual(self.manager.error_code, "publisher_cleanup_unconfirmed")
        self.assertTrue(self.manager._poisoned)

    async def test_consumer_failure_falls_back_to_standby_no_reactivation(self):
        await self.live()
        self.consumer.active = self.consumer.frame_ready = False
        self.consumer.failed.set()
        await self.settle(20)
        self.assertFalse(self.engine.video)
        self.assertFalse(self.manager.motion)
        self.assertEqual(self.consumer.open_calls, 1)
        self.assertEqual(self.manager.error_code, "consumer_failed")

    async def test_onvif_start_failure_stops_owned_server(self):
        self.onvif.fail_start = True
        with self.assertRaises(RuntimeError): await self.manager.start()
        self.assertFalse(self.engine.started)
        self.assertEqual(self.consumer.open_calls, 0)

    async def test_event_filter_and_duplicate_do_not_renew(self):
        self.settings["trigger_mode"] = "events"
        self.manager._events = policy.EventAdmission(self.settings["event_filter"], "event_at", clock=self.clock)
        await self.manager.start()
        self.clock.now = 101
        event = {"entry_id": "selected", "event_key": "door_unlock_started", "event_at": 101}
        self.hass.bus.emit("fixture_event", event)
        await self.settle()
        self.assertEqual(self.manager.deadline, 131)
        self.clock.now = 103
        self.hass.bus.emit("fixture_event", event.copy())
        self.hass.bus.emit("fixture_event", {**event, "entry_id": "foreign", "event_at": 103})
        await self.settle()
        self.assertEqual(self.manager.deadline, 131)
        self.assertEqual(self.consumer.open_calls, 1)

    async def test_stop_unsubscribes_and_no_stored_activation_replayed(self):
        self.settings["trigger_mode"] = "events"
        await self.manager.start()
        await self.manager.stop()
        self.assertEqual(self.hass.bus.unsubscribed, 1)
        self.assertFalse(await self.manager.async_activate())

    async def test_reader_starts_before_first_ha_frame_but_motion_waits_for_both(self):
        self.consumer.frame_gate.clear()
        await self.manager.start()
        await self.manager.async_activate()
        await self.settle()
        self.assertIn("video_start", self.trace)
        self.assertTrue(self.engine.video)
        self.assertFalse(self.manager.motion)
        self.assertNotIn("motion_True", self.trace)
        self.consumer.frame_gate.set()
        await self.settle()
        self.assertTrue(self.manager.motion)
        self.assertLess(self.trace.index("frame_ready"), self.trace.index("motion_True"))

    async def test_first_ha_frame_without_mux_is_not_motion(self):
        self.engine.start_gate.clear()
        await self.manager.start()
        await self.manager.async_activate()
        await self.settle()
        self.assertTrue(self.consumer.frame_ready)
        self.assertFalse(self.engine.video)
        self.assertFalse(self.manager.motion)
        self.engine.start_gate.set()
        await self.settle()
        self.assertTrue(self.manager.motion)

    async def test_worker_error_cancels_frame_waiter_without_retry(self):
        self.consumer.frame_gate.clear()
        self.engine.fail_start_video = True
        await self.manager.start()
        await self.manager.async_activate()
        await self.settle(80)
        self.assertEqual(self.consumer.open_calls, 1)
        self.assertEqual(self.manager.deadline, 0)
        self.assertNotIn("motion_True", self.trace)
        self.assertLess(self.trace.index("frame_wait_done"), self.trace.index("consumer_close"))
        self.assertLess(self.trace.index("video_stop"), self.trace.index("consumer_close"))

    async def test_frame_error_joins_pending_publisher_before_rtc_close(self):
        self.consumer.fail_frame = True
        self.engine.start_gate.clear()
        await self.manager.start()
        await self.manager.async_activate()
        await self.settle()
        self.assertLess(self.trace.index("video_cancel_joined"), self.trace.index("video_stop"))
        self.assertLess(self.trace.index("video_stop"), self.trace.index("consumer_close"))
        self.assertFalse(self.manager.motion)

    async def test_peer_failure_after_frame_while_publisher_pending_closes_promptly(self):
        self.engine.start_gate.clear()
        await self.manager.start()
        await self.manager.async_activate()
        await self.settle()
        self.consumer.active = self.consumer.frame_ready = False
        self.consumer.failed.set()
        await self.settle()
        self.assertIn("video_cancel_joined", self.trace)
        self.assertFalse(self.consumer.active)
        self.assertEqual(self.manager.deadline, 0)
        self.assertNotIn("motion_True", self.trace)

    async def test_manual_during_begin_cancels_open_outside_lock(self):
        self.consumer.open_gate.clear()
        await self.manager.start()
        await self.manager.async_activate()
        await self.settle()
        await self.manager.async_deactivate()
        self.assertNotIn("video_start", self.trace)
        self.assertFalse(self.manager.motion)
        self.assertEqual(self.manager.deadline, 0)

    async def test_expiry_during_begin_cancels_open_without_retry(self):
        self.consumer.open_gate.clear()
        await self.manager.start()
        await self.manager.async_activate()
        await self.settle()
        self.clock.now = 131
        self.manager._expired()
        await self.settle()
        self.assertNotIn("video_start", self.trace)
        self.assertFalse(self.manager.motion)
        self.assertEqual(self.consumer.open_calls, 1)

    async def test_cancellation_joins_blocked_publisher_before_consumer_close(self):
        self.engine.start_gate.clear()
        self.engine.cancel_gate.clear()
        self.consumer.frame_gate.clear()
        await self.manager.start()
        await self.manager.async_activate()
        await self.settle()
        first = asyncio.create_task(self.manager.async_deactivate())
        await self.settle()
        second = asyncio.create_task(self.manager.async_deactivate())
        await self.settle()
        self.assertEqual(self.consumer.close_calls, 0)
        self.assertFalse(first.done())
        self.engine.cancel_gate.set()
        await asyncio.gather(first, second)
        self.assertLess(self.trace.index("video_cancel_joined"), self.trace.index("consumer_close"))

    async def test_renewal_during_expiry_cleanup_restarts_only_new_activation(self):
        self.engine.start_gate.clear()
        self.engine.cancel_gate.clear()
        self.consumer.frame_gate.clear()
        await self.manager.start()
        await self.manager.async_activate()
        await self.settle()
        self.clock.now = 130
        self.manager._expired()
        await self.settle()
        self.clock.now = 131
        await self.manager.async_activate()
        self.engine.start_gate.set(); self.engine.cancel_gate.set(); self.consumer.frame_gate.set()
        await self.settle(80)
        self.assertEqual(self.manager.deadline, 161)
        self.assertEqual(self.consumer.open_calls, 2)
        self.assertEqual(self.manager.status, "live")

    async def test_old_attempt_failure_preserves_new_authorized_activation(self):
        self.engine.start_gate.clear()
        self.engine.fail_start_video = self.engine.fail_once = True
        await self.manager.start()
        await self.manager.async_activate()
        await self.settle()
        self.clock.now = 110
        await self.manager.async_activate()
        self.engine.start_gate.set()
        await self.settle(80)
        self.assertEqual(self.manager.deadline, 140)
        self.assertEqual(self.consumer.open_calls, 2)
        self.assertTrue(self.manager.motion)

    async def test_stale_session_failure_cannot_clear_later_session(self):
        await self.live()
        old_session, old_activation = self.consumer.session_id, self.manager._activation
        await self.manager.async_deactivate()
        await self.manager.async_activate()
        await self.settle()
        stops = self.engine.video_stops
        await self.manager._finish("worker_failed", clear=True, expected_session=old_session,
                                   expected_activation=old_activation)
        self.assertEqual(self.engine.video_stops, stops)
        self.assertTrue(self.manager.motion)
        self.assertEqual(self.manager.deadline, 130)

    async def test_stale_publisher_failure_cannot_clear_replacement_on_same_session(self):
        await self.live()
        old_generation = self.manager._publisher_generation
        self.clock.now = 131
        self.engine.video = False
        self.engine.callback("expired")
        await self.manager.async_activate()
        await self.settle()
        stops = self.engine.video_stops
        await self.manager._finish("worker_failed", clear=True, expected_session=self.consumer.session_id,
                                   expected_activation=self.manager._activation, expected_publisher=old_generation)
        self.assertEqual(self.engine.video_stops, stops)
        self.assertTrue(self.manager.motion)
        self.assertEqual(self.manager.deadline, 161)

    async def test_motion_off_before_blocking_consumer_close(self):
        await self.live()
        self.consumer.close_gate.clear()
        task = asyncio.create_task(self.manager.async_deactivate())
        await self.settle()
        self.assertFalse(task.done())
        self.assertFalse(self.engine.video)
        self.assertFalse(self.manager.motion)
        self.assertFalse(self.onvif.motion)
        self.consumer.close_gate.set()
        await task
        self.assertLess(self.trace.index("motion_False"), self.trace.index("consumer_close"))

    async def test_failure_cleanup_does_not_reassign_error_to_new_activation(self):
        await self.live()
        self.consumer.close_gate.clear()
        finish = asyncio.create_task(self.manager._finish("worker_failed", clear=True))
        await self.settle()
        self.clock.now = 110
        await self.manager.async_activate()
        self.consumer.close_gate.set()
        await finish
        await self.settle()
        self.assertTrue(self.manager.motion)
        self.assertIsNone(self.manager.error_code)
        self.assertEqual(self.manager.deadline, 140)

    async def test_open_failure_cleanup_does_not_reassign_error_to_new_activation(self):
        self.engine.fail_start_video = self.engine.fail_once = True
        self.consumer.close_gate.clear()
        await self.manager.start()
        await self.manager.async_activate()
        await self.settle()
        self.clock.now = 110
        await self.manager.async_activate()
        self.consumer.close_gate.set()
        await self.settle(80)
        self.assertTrue(self.manager.motion)
        self.assertIsNone(self.manager.error_code)
        self.assertEqual(self.manager.deadline, 140)

    async def test_real_consumer_failure_fence_with_pending_owned_reader(self):
        spec = importlib.util.spec_from_file_location("manager_consumer_fixture", BASE / "test_consumer.py")
        fixtures = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = fixtures
        spec.loader.exec_module(fixtures)
        backend = fixtures.FakeBackend()
        backend.auto_frame = False
        self.consumer = fixtures.Consumer(self.hass, backend=backend, managed_go2rtc=True,
                                         receive_audio=False, open_timeout=1.0, frame_timeout=0.5)
        self.manager.consumer = self.consumer
        self.engine.start_gate.clear()
        self.engine.cancel_gate.clear()
        await self.manager.start()
        await self.manager.async_activate()
        await self.settle(60)
        self.assertIn("video_start", self.trace)
        peer = backend.peers[0]
        session_id = self.consumer.session_id
        peer.video.queue.put_nowait(RuntimeError("PRIVATE_MEDIA_FAILURE"))
        await self.settle(60)
        self.assertFalse(self.consumer.active)
        self.assertEqual(backend.camera.closed_ids, [])
        self.assertEqual(peer.close_count, 0)
        self.assertIn(session_id, backend.camera.sessions)
        self.engine.cancel_gate.set()
        await self.settle(80)
        self.assertEqual(backend.camera.closed_ids, [session_id])
        self.assertEqual(peer.close_count, 1)
        self.assertEqual(backend.camera.sessions, {"other-ui-session"})
        self.assertFalse(self.manager.motion)
        self.assertTrue(self.consumer.cleanup_ok)


class PolicyTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.policy = policy.EventAdmission({"entry_id": "selected", "event_key": ["ring", "unlock"]},
                                            "event_at", clock=self.clock)
    def event(self, **change):
        return {"entry_id": "selected", "event_key": "unlock", "event_at": self.clock.now, **change}

    def test_exact_match_and_or(self):
        self.assertTrue(self.policy.accept(self.event(event_key="ring")))
        self.assertFalse(self.policy.accept(self.event(entry_id="foreign")))

    def test_replay_uses_canonical_json_order(self):
        value = self.event()
        self.assertTrue(self.policy.accept(value))
        self.assertFalse(self.policy.accept(dict(reversed(list(value.items())))))

    def test_future_stale_and_prestartup_rejected(self):
        self.assertFalse(self.policy.accept(self.event(event_at=101)))
        self.assertFalse(self.policy.accept(self.event(event_at=99)))
        self.clock.now = 110
        self.assertFalse(self.policy.accept(self.event(event_at=100)))

    def test_typed_matching_boolean_not_integer(self):
        admission = policy.EventAdmission({"value": 1}, "event_at", clock=self.clock)
        self.assertFalse(admission.accept({"value": True, "event_at": 100}))
        self.assertTrue(admission.accept({"value": 1, "event_at": 100}))

    def test_cache_saturation_does_not_evict_recent_replay(self):
        for index in range(512): self.assertTrue(self.policy.accept(self.event(index=index)))
        self.assertFalse(self.policy.accept(self.event(index=513)))
        self.assertFalse(self.policy.accept(self.event(index=0)))
        self.clock.now = 111
        self.assertTrue(self.policy.accept(self.event(index=513)))

    def test_payload_bound_and_nonfinite_rejected(self):
        self.assertFalse(self.policy.accept(self.event(extra="x" * 8192)))
        self.assertFalse(self.policy.accept(self.event(event_at=float("nan"))))
        self.assertFalse(self.policy.accept(self.event(extra=float("inf"))))

    def test_naive_timestamp_rejected(self):
        self.assertIsNone(policy.timestamp("2026-10-11T10:00:00"))
        self.assertIsNotNone(policy.timestamp("2026-10-11T10:00:00+00:00"))


class EngineContractTests(unittest.TestCase):
    """Exercise the real engine's pure state contract, without starting it."""

    def setUp(self):
        self.clock = Clock()
        self.engine = engine_module.MediaEngine(
            SimpleNamespace(), "fixture-entry",
            {"bind_ip": "127.0.0.1", "rtsp_port": 18557,
             "username": "fixture_reader", "password": "fixture-password-long",
             "allowed_clients": ["127.0.0.1"]}, clock=self.clock,
        )

    def test_manager_zero_deadline_clears_instead_of_throwing(self):
        self.engine.extend_deadline(130)
        self.engine.extend_deadline(0)
        self.assertEqual(self.engine.deadline, 0)

    def test_running_means_server_available_not_video_running(self):
        # Explicitly document the property used by HA availability.
        self.assertFalse(self.engine.running)
        self.engine._server = SimpleNamespace(poll=lambda: None)
        self.assertTrue(self.engine.running)
        self.assertIsNone(self.engine._worker)

    def test_zero_deadline_still_permitted_after_engine_fault(self):
        self.engine._fault = True
        self.engine.extend_deadline(0)
        self.assertEqual(self.engine.deadline, 0)

    def test_zero_deadline_still_permitted_after_closed(self):
        self.engine._closed = True
        self.engine.extend_deadline(0)
        self.assertEqual(self.engine.deadline, 0)


def load_integration_boundary():
    """Load real __init__ with import-only fakes; factories are patched by tests."""
    name = "exporter_setup_review_fixture"
    package = ModuleType(name)
    package.__path__ = [str(BASE)]
    const = load("const")
    flow = ModuleType(name + ".config_flow")
    flow.entry_settings = lambda entry: entry.data
    flow.normalize_input = lambda data: data
    imports = {name: package, name + ".const": const, name + ".config_flow": flow}
    for leaf, exported in (("consumer", "HACameraConsumer"), ("engine", "MediaEngine"),
                           ("manager", "ExportManager"), ("onvif_server", "ONVIFServer")):
        module = ModuleType(name + "." + leaf)
        setattr(module, exported, lambda *args, **kwargs: None)
        imports[module.__name__] = module
    vol = ModuleType("voluptuous")
    vol.Schema = lambda schema: schema
    vol.Required = lambda field: field
    imports["voluptuous"] = vol
    for leaf in ("const", "core", "exceptions"):
        imports["homeassistant." + leaf] = ModuleType("homeassistant." + leaf)
    imports["homeassistant.const"].EVENT_HOMEASSISTANT_STOP = "ha_stop"
    imports["homeassistant.core"].callback = lambda callback: callback
    for error in ("ConfigEntryNotReady", "ServiceValidationError"):
        setattr(imports["homeassistant.exceptions"], error, type(error, (Exception,), {}))
    spec = importlib.util.spec_from_file_location(name, BASE / "__init__.py", submodule_search_locations=[str(BASE)])
    module = importlib.util.module_from_spec(spec)
    imports[name] = module
    with mock.patch.dict(sys.modules, imports):
        spec.loader.exec_module(module)
    return module


class SetupBoundaryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.boundary = load_integration_boundary()
        self.entry = SimpleNamespace(
            entry_id="fixture-entry", runtime_data=None,
            data={"virtual_id": "fixture-uuid", "bind_ip": "127.0.0.1", "onvif_port": 1986,
                  "rtsp_port": 18557, "allowed_clients": ["127.0.0.1"],
                  "username": "fixture_reader", "password": "fixture-password-long", "name": "Fixture"},
            async_on_unload=lambda callback: None,
        )
        self.hass = SimpleNamespace(
            data={"ha_onvif_exporter": {}},
            bus=SimpleNamespace(async_listen_once=lambda *args: (lambda: None)),
            config_entries=SimpleNamespace(async_forward_entry_setups=mock.AsyncMock(),
                                           async_unload_platforms=mock.AsyncMock(return_value=True)),
        )
        self.fake_manager = SimpleNamespace(start=mock.AsyncMock(), stop=mock.AsyncMock(return_value=True))
        self.factories = [mock.patch.object(self.boundary, key, return_value=value) for key, value in (
            ("HACameraConsumer", object()), ("MediaEngine", object()), ("ONVIFServer", object()),
            ("ExportManager", self.fake_manager))]
        self.started_factories = [patcher.start() for patcher in self.factories]

    async def asyncTearDown(self):
        for patcher in reversed(self.factories): patcher.stop()

    async def test_unconfirmed_unload_keeps_original_manager(self):
        self.entry.runtime_data = self.fake_manager
        self.hass.data["ha_onvif_exporter"][self.entry.entry_id] = self.fake_manager
        self.fake_manager.stop.return_value = False
        self.assertFalse(await self.boundary.async_unload_entry(self.hass, self.entry))
        self.assertIs(self.hass.data["ha_onvif_exporter"][self.entry.entry_id], self.fake_manager)

    async def test_failed_cleanup_cannot_be_bypassed_by_repeated_setup(self):
        original = SimpleNamespace(stop=mock.AsyncMock(return_value=False), _poisoned=True)
        self.entry.runtime_data = original
        self.hass.data["ha_onvif_exporter"][self.entry.entry_id] = original
        try:
            await self.boundary.async_setup_entry(self.hass, self.entry)
        except Exception:
            pass
        self.assertEqual(self.started_factories[0].call_count, 0,
                         "Repeated setup created a fresh consumer across an unconfirmed cleanup fence")
        self.assertIs(self.hass.data["ha_onvif_exporter"][self.entry.entry_id], original)

    async def test_platform_setup_failure_retains_unconfirmed_manager(self):
        self.hass.config_entries.async_forward_entry_setups.side_effect = RuntimeError("PUBLIC_FAKE_SETUP_FAILURE")
        self.fake_manager.stop.return_value = False
        with self.assertRaises(RuntimeError):
            await self.boundary.async_setup_entry(self.hass, self.entry)
        self.assertIs(self.hass.data["ha_onvif_exporter"].get(self.entry.entry_id), self.fake_manager,
                      "Failed platform setup discarded a manager with unconfirmed cleanup")

    async def test_successful_unload_removes_owned_manager(self):
        self.entry.runtime_data = self.fake_manager
        self.hass.data["ha_onvif_exporter"][self.entry.entry_id] = self.fake_manager
        self.assertTrue(await self.boundary.async_unload_entry(self.hass, self.entry))
        self.assertNotIn(self.entry.entry_id, self.hass.data["ha_onvif_exporter"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
