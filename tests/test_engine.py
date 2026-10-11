"""Actual media-engine lifecycle tests with owned child fakes; no camera/network."""
from __future__ import annotations

import asyncio
import base64
import hashlib
import importlib.util
import json
from pathlib import Path
import queue
import subprocess
import sys
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

COMPONENT = Path(__file__).resolve().parents[1] / "custom_components" / "ha_onvif_exporter"
SPEC = importlib.util.spec_from_file_location("exporter_engine_review", COMPONENT / "engine.py")
engine = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = engine
SPEC.loader.exec_module(engine)
SOURCE = "rtsp://127.0.0.1:18554/camera_fixture"
PASSWORD = "test-only-reader-password"


def settings(**changes):
    return {"bind_ip": "10.9.8.7", "rtsp_port": 18557, "username": "fixture_reader",
            "password": PASSWORD, "allowed_clients": ["10.9.8.1", "10.9.7.56"], **changes}


class Clock:
    def __init__(self):
        self.now = 100.0
    def __call__(self):
        return self.now


class Pipe:
    def __init__(self):
        self.lines = queue.Queue()
        self.closed = False
    def readline(self, size):
        return self.lines.get(timeout=2.0)
    def send(self, value):
        self.lines.put(value)
    def close(self):
        self.closed = True


class Sink:
    def __init__(self):
        self.contents = b""
        self.closed = False
        self.short = False
    def write(self, content):
        self.contents += content
        return len(content) - 1 if self.short else len(content)
    def close(self):
        self.closed = True


class Process:
    def __init__(self, *, kill_works=True, wait_works=True):
        self.stdin, self.stdout = Sink(), Pipe()
        self.returncode = None
        self.kills = self.waits = 0
        self.kill_works, self.wait_works = kill_works, wait_works
    def poll(self):
        return self.returncode
    def kill(self):
        self.kills += 1
        if not self.kill_works:
            raise PermissionError("fixture-raw-error-must-not-escape")
        self.returncode = -9
        self.stdout.send(b"")
    def wait(self, timeout):
        self.waits += 1
        if not self.wait_works or self.returncode is None:
            raise subprocess.TimeoutExpired("fixture-only", timeout)
        return self.returncode


class Factory:
    def __init__(self, process=None, initial=None):
        self.process = process or Process()
        self.initial = initial
        self.calls = []
        self.entered = threading.Event()
        self.release = None
        self.failure = False
    def __call__(self, argv, **kwargs):
        self.calls.append((argv, kwargs))
        self.entered.set()
        if self.release is not None:
            self.release.wait(2.0)
        if self.failure:
            raise OSError(PASSWORD)
        if self.initial is not None:
            self.process.stdout.send(self.initial)
        return self.process


class ConfigTests(unittest.TestCase):
    def test_worker_and_engine_diagnostic_allowlists_are_identical(self):
        spec = importlib.util.spec_from_file_location("worker_codes_review", COMPONENT / "media_worker.py")
        worker = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(worker)
        self.assertEqual(engine.WORKER_ERROR_CODES, worker.WORKER_ERROR_CODES)

    def test_precise_roles_peer_acl_and_no_extra_listeners(self):
        validated = engine.validate_settings(settings())
        config = engine.media_config(validated, "a" * 64, Path("/config/private/standby.mp4"))
        self.assertEqual(config["rtspAddress"], "10.9.8.7:18557")
        self.assertEqual(config["rtspTransports"], ["tcp"])
        for name in ("api", "metrics", "pprof", "playback", "rtmp", "hls", "webrtc", "srt", "moq"):
            self.assertIs(config[name], False)
        read, publish = config["authInternalUsers"]
        self.assertEqual(read["permissions"], [{"action": "read", "path": "standby"}])
        self.assertEqual(read["ips"], ["10.9.8.1/32", "10.9.7.56/32"])
        self.assertEqual(publish["permissions"], [{"action": "publish", "path": "standby"}])
        self.assertEqual(publish["ips"], ["10.9.8.7/32"])
        self.assertEqual(read["user"], engine.password_hash("fixture_reader"))
        self.assertEqual(read["pass"], engine.password_hash(PASSWORD))
        self.assertEqual(publish["user"], engine.password_hash("window_publisher"))
        self.assertEqual(publish["pass"], engine.password_hash("a" * 64))
        self.assertNotIn(PASSWORD, json.dumps(config))
        self.assertNotIn("a" * 64, json.dumps(config))
        path = config["paths"]["standby"]
        self.assertTrue(path["alwaysAvailable"])
        self.assertFalse(path["overridePublisher"])
        self.assertFalse(path["record"])
        self.assertFalse(path["alwaysAvailableRecorded"])
        self.assertEqual(set(config["paths"]), {"standby"})

    def test_sha256_format_matches_primary_contract(self):
        self.assertEqual(engine.password_hash("test"), "sha256:" + base64.b64encode(hashlib.sha256(b"test").digest()).decode())

    def test_invalid_auth_and_network_settings_fail_closed(self):
        changes = [{"username": "any"}, {"username": "window_publisher"}, {"username": "u:p"},
                   {"password": "x" * 15}, {"password": "é" * 16}, {"allowed_clients": []},
                   {"allowed_clients": ["10.0.0.0/8"]}, {"allowed_clients": ["0.0.0.0"]},
                   {"allowed_clients": [123]}, {"bind_ip": "0.0.0.0"}, {"bind_ip": "::1"},
                   {"rtsp_port": True}, {"rtsp_port": "18557"}, {"rtsp_port": 1023}]
        for change in changes:
            with self.subTest(change=change):
                with self.assertRaises(engine.EngineError) as error:
                    engine.validate_settings(settings(**change))
                self.assertEqual(str(error.exception), "invalid_settings")
                self.assertNotIn(PASSWORD, str(error.exception))

    def test_bundled_chunks_and_standby_reconstruct_pinned_assets(self):
        archive = engine._read_archive(COMPONENT / "assets")
        self.assertEqual(len(archive), engine.ARCHIVE_SIZE)
        self.assertEqual(hashlib.sha256(archive).hexdigest(), engine.ARCHIVE_SHA256)
        standby = engine._read_pinned(COMPONENT / "assets" / "standby.mp4", engine.STANDBY_SHA256, 16 * 1024 * 1024)
        self.assertEqual(len(standby), 25145)

    def test_tampered_asset_is_rejected(self):
        with self.assertRaises(engine.EngineError):
            engine._read_pinned(COMPONENT / "assets" / "standby.mp4", "0" * 64, 16 * 1024 * 1024)


class EngineTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.clock = Clock()
        self.factory = Factory(initial=b'{"frames":1}\n')
        self.hass = SimpleNamespace(config=SimpleNamespace(path=lambda name: "/config/" + name))
        self.media = engine.MediaEngine(self.hass, "entry_fixture", settings(), popen=self.factory,
                                        clock=self.clock, open_timeout=0.3, reap_timeout=0.1)
        self.media._loop = asyncio.get_running_loop()
        self.media._runtime = Path("/config/private-fixture")
        self.server = Process()
        self.media._server = self.server
        self.events = []
        self.media.set_callback(self.events.append)

    async def asyncTearDown(self):
        self.factory.process.kill_works = self.factory.process.wait_works = True
        if self.factory.release is not None:
            self.factory.release.set()
        self.server.kill_works = self.server.wait_works = True
        await self.media.stop()
        await asyncio.sleep(0)

    async def wait_spawn(self):
        for _ in range(100):
            if self.media._worker is not None and self.media._worker.spawn_done.is_set():
                return
            await asyncio.sleep(0.002)
        self.fail("fake child did not finish spawning")

    async def activate(self):
        self.media.extend_deadline(self.clock() + 30)
        await self.media.start_video(SOURCE)

    async def test_start_requires_first_numeric_successful_mux_progress(self):
        self.factory.initial = None
        self.media.extend_deadline(130)
        task = asyncio.create_task(self.media.start_video(SOURCE))
        await self.wait_spawn()
        self.assertFalse(task.done())
        self.assertEqual(self.media.frames, 0)
        self.assertFalse(self.media.video_running)
        self.factory.process.stdout.send(b'{"frames":1}\n')
        await task
        self.assertEqual(self.media.frames, 1)
        self.assertTrue(self.media.video_running)

    async def test_credentials_only_stdin_no_argv_or_environment(self):
        await self.activate()
        argv, options = self.factory.calls[0]
        self.assertNotIn(PASSWORD, str(argv))
        self.assertNotIn(self.media._publisher_password, str(argv))
        self.assertNotIn(SOURCE, str(argv))
        self.assertEqual(options["env"], {"LANG": "C.UTF-8", "TZ": "UTC"})
        self.assertEqual(options["stderr"], subprocess.DEVNULL)
        self.assertTrue(options["close_fds"])
        self.assertIn("process_guard.py", argv[3])
        data = json.loads(self.factory.process.stdin.contents)
        self.assertEqual(set(data), {"source", "sink_host", "rtsp_port", "publisher_user", "publisher_password"})
        self.assertEqual(data["source"], SOURCE)
        self.assertEqual(data["publisher_user"], "window_publisher")
        self.assertRegex(data["publisher_password"], r"^[0-9a-f]{64}$")
        self.assertTrue(self.factory.process.stdin.closed)

    async def test_stop_reaps_exact_worker_before_returning_and_preserves_server(self):
        await self.activate()
        self.assertTrue(await self.media.stop_video())
        self.assertGreaterEqual(self.factory.process.kills, 1)
        self.assertGreaterEqual(self.factory.process.waits, 1)
        self.assertEqual(self.server.kills, 0)
        self.assertTrue(self.media.running)
        self.assertFalse(self.media.video_running)
        self.assertEqual(self.media.deadline, 0)
        self.assertIsNone(self.media._worker)

    async def test_zero_cancels_active_worker_but_not_standby_server(self):
        self.media.extend_deadline(0)
        self.media._watchdog_once()
        self.assertEqual(self.events, [])
        self.assertEqual(self.server.kills, 0)
        await self.activate()
        self.media.extend_deadline(0)
        self.media._watchdog_once()
        await asyncio.sleep(0)
        self.assertEqual(self.events, ["expired"])
        self.assertEqual(self.factory.process.poll(), -9)
        self.assertTrue(self.media.running)

    async def test_latest_activation_extends_not_frames_or_readers(self):
        await self.activate()
        self.clock.now = 115
        self.media.extend_deadline(145)
        self.clock.now = 130
        self.factory.process.stdout.send(b'{"frames":2}\n')
        await asyncio.sleep(0.02)
        self.media._watchdog_once()
        self.assertEqual(self.factory.process.kills, 0)
        self.assertEqual(self.media.deadline, 145)
        self.clock.now = 145
        self.media._watchdog_once()
        await asyncio.sleep(0)
        self.assertEqual(self.events, ["expired"])
        self.assertEqual(self.factory.process.kills, 1)
        self.assertEqual(self.server.kills, 0)

    async def test_independent_watchdog_kills_while_ha_loop_is_blocked(self):
        await self.activate()
        self.media._watcher = threading.Thread(target=self.media._watchdog, daemon=True)
        self.media._watcher.start()
        self.clock.now = 131
        time.sleep(0.13)  # Deliberately blocks only this offline test's event loop.
        self.assertEqual(self.factory.process.poll(), -9)
        self.assertEqual(self.server.kills, 0)
        await asyncio.sleep(0)
        self.assertEqual(self.events, ["expired"])

    async def test_first_frame_after_expiry_does_not_mark_ready(self):
        self.factory.initial = None
        self.media.extend_deadline(130)
        task = asyncio.create_task(self.media.start_video(SOURCE))
        await self.wait_spawn()
        self.clock.now = 130
        self.factory.process.stdout.send(b'{"frames":1}\n')
        with self.assertRaises(engine.EngineError):
            await task
        self.assertEqual(self.media.frames, 0)
        self.assertFalse(self.media.video_running)

    async def test_late_popen_handle_retained_and_killed_before_stdin_admission(self):
        self.factory.initial = None
        self.factory.release = threading.Event()
        self.media.extend_deadline(130)
        task = asyncio.create_task(self.media.start_video(SOURCE))
        await asyncio.to_thread(self.factory.entered.wait, 0.2)
        self.clock.now = 130
        self.media._watchdog_once()
        self.factory.release.set()
        with self.assertRaises(engine.EngineError):
            await task
        self.assertEqual(self.factory.process.stdin.contents, b"")
        self.assertEqual(self.factory.process.poll(), -9)
        self.assertIsNone(self.media._worker)

    async def test_failed_reap_latches_fault_blocks_new_worker(self):
        await self.activate()
        self.factory.process.wait_works = False
        self.assertFalse(await self.media.stop_video())
        self.assertIsNotNone(self.media._worker)
        self.assertFalse(self.media.running)
        with self.assertRaises(engine.EngineError):
            self.media.extend_deadline(140)
        with self.assertRaises(engine.EngineError):
            await self.media.start_video(SOURCE)
        self.factory.process.wait_works = True
        self.assertTrue(await self.media.stop_video())
        self.assertFalse(self.media.running)  # Never silently clear a fault.

    async def test_kill_failure_cannot_claim_cleanup(self):
        await self.activate()
        self.factory.process.kill_works = False
        self.assertFalse(await self.media.stop_video())
        self.assertIsNotNone(self.media._worker)
        self.assertIsNone(self.factory.process.poll())

    async def test_worker_failure_does_not_renew_deadline(self):
        await self.activate()
        self.factory.process.stdout.send(b"")
        await asyncio.sleep(0.02)
        self.assertEqual(self.events, ["worker_failed"])
        self.assertEqual(self.media.deadline, 130)
        self.assertEqual(self.factory.process.poll(), -9)

    async def test_server_failure_kills_worker_and_emits_one_safe_notice(self):
        await self.activate()
        self.server.returncode = 1
        self.media._watchdog_once()
        self.media._watchdog_once()
        await asyncio.sleep(0)
        self.assertEqual(self.events, ["server_failed"])
        self.assertEqual(self.factory.process.poll(), -9)
        self.assertFalse(self.media.running)

    async def test_invalid_progress_does_not_become_readiness(self):
        for raw in (b'{"frames":true}\n', b'{"frames":0}\n', b'{"frames":1,"frames":2}\n',
                    b'{"frames":1,"password":"not-output"}\n', b"\xff\n", b"x" * 257):
            with self.subTest(raw_type=len(raw)):
                factory = Factory(initial=raw)
                other = engine.MediaEngine(self.hass, "other", settings(), popen=factory, clock=self.clock, open_timeout=0.2, reap_timeout=0.1)
                other._loop, other._runtime, other._server = asyncio.get_running_loop(), self.media._runtime, Process()
                other.extend_deadline(130)
                with self.assertRaises(engine.EngineError):
                    await other.start_video(SOURCE)
                self.assertEqual(other.frames, 0)
                self.assertTrue(await other.stop())

    async def test_progress_regression_fails_closed(self):
        await self.activate()
        self.factory.process.stdout.send(b'{"frames":1}\n')
        await asyncio.sleep(0.02)
        self.assertEqual(self.events, ["worker_failed"])
        self.assertEqual(self.factory.process.poll(), -9)

    async def test_no_deadline_no_capture_and_no_native_source(self):
        with self.assertRaises(engine.EngineError):
            await self.media.start_video(SOURCE)
        self.assertEqual(self.factory.calls, [])
        self.media.extend_deadline(130)
        for source in ("rtsp://10.0.0.1:6554/doorbell", "rtsp://localhost:18554/x", SOURCE + "?auth=x", "rtsp://u:p@127.0.0.1:18554/x"):
            with self.assertRaises(engine.EngineError):
                await self.media.start_video(source)
        self.assertEqual(self.factory.calls, [])

    async def test_popen_failure_and_short_write_are_sanitized(self):
        for failure, short in ((True, False), (False, True)):
            self.factory.failure, self.factory.process.stdin.short = failure, short
            self.media.extend_deadline(130)
            with self.assertRaises(engine.EngineError) as result:
                await self.media.start_video(SOURCE)
            self.assertEqual(str(result.exception), "worker_failed")
            self.assertNotIn(PASSWORD, str(result.exception))
            self.assertIsNone(self.media._worker)

    async def test_cancel_open_stops_and_reaps_owned_child(self):
        self.factory.initial = None
        self.media.extend_deadline(130)
        task = asyncio.create_task(self.media.start_video(SOURCE))
        await self.wait_spawn()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(self.factory.process.poll(), -9)
        self.assertIsNone(self.media._worker)

    async def test_stop_before_first_frame_wakes_open_and_confirms_cleanup(self):
        self.factory.initial = None
        self.media.extend_deadline(130)
        task = asyncio.create_task(self.media.start_video(SOURCE))
        await self.wait_spawn()
        self.assertTrue(await self.media.stop_video())
        with self.assertRaises(engine.EngineError):
            await task
        self.assertEqual(self.factory.process.poll(), -9)

    async def test_stale_queued_notice_cannot_target_new_generation(self):
        await self.activate()
        old = self.media._worker
        self.media._notify("expired", old)
        with self.media._lock:
            self.media._worker = engine._Worker(asyncio.get_running_loop().create_future())
        await asyncio.sleep(0)
        self.assertEqual(self.events, [])
        self.media._worker = old

    async def test_new_deadline_cannot_undo_a_physical_expiry_kill(self):
        await self.activate()
        self.clock.now = 130
        self.media._watchdog_once()
        self.media.extend_deadline(160)
        await asyncio.sleep(0)
        self.assertEqual(self.events, ["expired"])
        self.assertFalse(self.media.video_running)
        self.assertEqual(self.factory.process.poll(), -9)

    async def test_stop_is_confirmed_only_after_both_children_reaped(self):
        await self.activate()
        self.assertTrue(await self.media.stop())
        self.assertGreaterEqual(self.factory.process.waits, 1)
        self.assertGreaterEqual(self.server.waits, 1)
        self.assertTrue(self.media._closed)
        with self.assertRaises(engine.EngineError):
            await self.media.start()

    async def test_invalid_deadlines_rejected_and_older_extension_cannot_shorten(self):
        for value in (True, -1, 100, 99, float("nan"), float("inf")):
            with self.assertRaises(engine.EngineError):
                self.media.extend_deadline(value)
        self.media.extend_deadline(150)
        self.media.extend_deadline(140)
        self.assertEqual(self.media.deadline, 150)

    async def test_zero_is_idempotent_after_fault_stopping_and_closed(self):
        for attribute in ("_fault", "_stopping", "_closed"):
            self.media._deadline = 130
            setattr(self.media, attribute, True)
            self.media.extend_deadline(0)
            self.media.extend_deadline(0)
            self.assertEqual(self.media.deadline, 0)
            self.assertTrue(getattr(self.media, attribute))
            setattr(self.media, attribute, False)

    async def test_fixed_worker_error_is_not_readiness_and_survives_cleanup(self):
        for code in engine.WORKER_ERROR_CODES:
            with self.subTest(code=code):
                factory = Factory(initial=json.dumps({"error": code}).encode() + b"\n")
                other = engine.MediaEngine(self.hass, "diagnostic", settings(), popen=factory, clock=self.clock,
                                           open_timeout=0.2, reap_timeout=0.1)
                other._loop, other._runtime, other._server = asyncio.get_running_loop(), self.media._runtime, Process()
                other.extend_deadline(130)
                with self.assertRaises(engine.EngineError) as caught:
                    await other.start_video(SOURCE)
                self.assertEqual(str(caught.exception), "worker_failed")
                self.assertEqual(other.frames, 0)
                self.assertEqual(other.failure_code, code)
                self.assertIsNone(other._worker)
                self.assertTrue(await other.stop())
                self.assertEqual(other.failure_code, code)

    async def test_malformed_or_unapproved_worker_error_never_becomes_detail(self):
        private = "rtsp://user:PRIVATE_PASSWORD@192.0.2.1/path"
        lines = [json.dumps(value).encode() + b"\n" for value in
                 ({"error": private}, {"error": True}, {"error": [private]},
                  {"error": "worker_internal", "frames": 1}, {"error": "config_sink", "url": private})]
        lines.extend((b'{"error":"config_sink","error":"config_read"}\n', b'\xff\n', b'x' * 257))
        for raw in lines:
            with self.subTest(line_size=len(raw)):
                factory = Factory(initial=raw)
                other = engine.MediaEngine(self.hass, "invalid_diagnostic", settings(), popen=factory, clock=self.clock,
                                           open_timeout=0.2, reap_timeout=0.1)
                other._loop, other._runtime, other._server = asyncio.get_running_loop(), self.media._runtime, Process()
                other.extend_deadline(130)
                with self.assertRaises(engine.EngineError):
                    await other.start_video(SOURCE)
                self.assertEqual(other.failure_code, "worker_failed")
                self.assertEqual(other.frames, 0)
                self.assertTrue(await other.stop())

    async def test_error_after_first_mux_notifies_generic_but_exposes_safe_detail(self):
        await self.activate()
        self.factory.process.stdout.send(b'{"error":"media_pipeline_decode_filter_mux"}\n')
        await asyncio.sleep(0.02)
        self.assertEqual(self.events, ["worker_failed"])
        self.assertEqual(self.media.failure_code, "media_pipeline_decode_filter_mux")
        self.assertTrue(await self.media.stop_video())
        self.assertEqual(self.media.failure_code, "media_pipeline_decode_filter_mux")

    async def test_watchdog_exit_before_pipe_drain_keeps_detail(self):
        record = engine._Worker(asyncio.get_running_loop().create_future())
        record.ready.add_done_callback(lambda future: future.exception())
        record.process = Process()
        record.process.returncode = 1
        record.process.stdout.send(b'{"error":"media_pipeline_source_open"}\n')
        record.spawn_done.set()
        self.media._worker = record
        self.media.extend_deadline(130)
        self.media._watchdog_once()
        self.assertEqual(self.media.failure_code, "worker_failed")
        self.media._read_progress(record)
        await asyncio.sleep(0)
        self.assertEqual(self.media.failure_code, "media_pipeline_source_open")
        self.assertEqual(self.events, ["worker_failed"])
        self.assertTrue(await self.media.stop_video())

    async def test_next_real_mux_clears_prior_detail_not_new_attempt_alone(self):
        self.factory.initial = b'{"error":"media_pipeline_source_open"}\n'
        self.media.extend_deadline(130)
        with self.assertRaises(engine.EngineError):
            await self.media.start_video(SOURCE)
        self.assertEqual(self.media.failure_code, "media_pipeline_source_open")
        self.factory = Factory(initial=None)
        self.media._popen = self.factory
        self.media.extend_deadline(130)
        task = asyncio.create_task(self.media.start_video(SOURCE))
        await self.wait_spawn()
        self.assertEqual(self.media.failure_code, "media_pipeline_source_open")
        self.factory.process.stdout.send(b'{"frames":1}\n')
        await task
        self.assertIsNone(self.media.failure_code)

    async def test_expiry_does_not_become_error_from_late_child_output(self):
        await self.activate()
        record = self.media._worker
        self.clock.now = 130
        self.media._watchdog_once()
        record.process.stdout.send(b'{"error":"media_pipeline_source_open"}\n')
        await asyncio.sleep(0.02)
        self.assertEqual(self.events, ["expired"])
        self.assertIsNone(self.media.failure_code)


if __name__ == "__main__":
    unittest.main()
