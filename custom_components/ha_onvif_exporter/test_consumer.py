"""Offline contract tests: fake HA camera and fake RTC peer; no network."""

import asyncio
from dataclasses import dataclass
import importlib.util
from pathlib import Path
import sys
import types
import unittest
from unittest import mock

_NAME = "ha_onvif_exporter_consumer_under_test"
spec = importlib.util.spec_from_file_location(_NAME, Path(__file__).with_name("consumer.py"))
module = importlib.util.module_from_spec(spec)
sys.modules[_NAME] = module
spec.loader.exec_module(module)
Consumer = module.HACameraConsumer
ConsumerError = module.ConsumerError


class FakeTrack:
    def __init__(self, kind):
        self.kind = kind
        self.queue = asyncio.Queue()

    async def recv(self):
        value = await self.queue.get()
        if isinstance(value, BaseException):
            raise value
        return value


class FakePeer:
    def __init__(self):
        self.handlers = {}
        self.transceivers = []
        self.connectionState = "new"
        self.localDescription = None
        self.remote = []
        self.candidates = []
        self.close_count = 0
        self.close_failure = False
        self.video = FakeTrack("video")
        self.audio = FakeTrack("audio")
        self.auto_frame = True
        self.remote_gate = None

    def on(self, name):
        def register(fn):
            self.handlers[name] = fn
            return fn
        return register

    def addTransceiver(self, kind, direction):
        self.transceivers.append((kind, direction))

    async def createOffer(self):
        return types.SimpleNamespace(sdp="v=0\nm=video\na=recvonly\nm=audio\na=recvonly", type="offer")

    async def setLocalDescription(self, offer):
        self.localDescription = offer

    async def setRemoteDescription(self, answer):
        if self.remote_gate is not None:
            await self.remote_gate.wait()
        self.remote.append(answer)
        self.handlers["track"](self.video)
        if ("audio", "recvonly") in self.transceivers:
            self.handlers["track"](self.audio)
        if self.auto_frame:
            self.video.queue.put_nowait("video-frame")
            self.audio.queue.put_nowait("audio-frame")

    async def addIceCandidate(self, candidate):
        self.candidates.append(candidate)

    async def close(self):
        self.close_count += 1
        if self.close_failure:
            raise RuntimeError("private-camera-url")
        self.connectionState = "closed"
        if "connectionstatechange" in self.handlers:
            self.handlers["connectionstatechange"]()


class FakeCamera:
    def __init__(self):
        self.available = True
        self.is_on = True
        self.supported = True
        self.calls = []
        self.closed_ids = []
        self.sessions = {"other-ui-session"}
        self.messages = [{"type": "answer", "answer": "v=0\na=sendonly"}]
        self.callback = None
        self.hang = False
        self.close_failure = False
        self.offer_entered = asyncio.Event()
        self.answer_gate = None

    async def async_handle_async_webrtc_offer(self, sdp, session_id, callback):
        self.calls.append((sdp, session_id))
        self.sessions.add(session_id)
        self.callback = callback
        self.offer_entered.set()
        if self.answer_gate is not None:
            await self.answer_gate.wait()
        for message in self.messages:
            callback(message)
        if self.hang:
            await asyncio.Event().wait()

    def close_webrtc_session(self, session_id):
        self.closed_ids.append(session_id)
        self.sessions.discard(session_id)
        if self.close_failure:
            raise RuntimeError("private-camera-url")


class FakeBackend:
    def __init__(self):
        self.camera = FakeCamera()
        self.peers = []
        self.alias = "rtsp://127.0.0.1:18554/bticino_c300x_abc"
        self.auto_frame = True
        self.resolve_failure = False

    def resolve_camera(self, hass, entity_id):
        if self.resolve_failure:
            raise RuntimeError("sensitive native url")
        return self.camera

    def supports_webrtc(self, camera):
        return camera.supported

    def source(self, camera):
        return self.alias

    def create_peer(self, camera):
        peer = FakePeer()
        peer.auto_frame = self.auto_frame
        self.peers.append(peer)
        return peer

    def description(self, sdp):
        return sdp

    def candidate(self, data):
        return dict(data)


class ContractTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.backend = FakeBackend()
        self.consumer = Consumer(None, backend=self.backend, managed_go2rtc=True,
                                 open_timeout=.3, frame_timeout=.3, callback_timeout=.1)

    async def asyncTearDown(self):
        await self.consumer.close()

    async def opened(self):
        return await self.consumer.open("camera.test")

    async def expect_error(self, code):
        with self.assertRaises(ConsumerError) as caught:
            await self.opened()
        self.assertEqual(caught.exception.code, code)

    async def test_open_waits_for_real_frame_not_answer_or_track(self):
        self.backend.auto_frame = False
        task = asyncio.create_task(self.opened())
        await self.backend.camera.offer_entered.wait()
        await asyncio.sleep(.01)
        self.assertFalse(task.done())
        self.assertFalse(self.consumer.frame_ready)
        self.backend.peers[0].video.queue.put_nowait("real-frame")
        info = await task
        self.assertTrue(self.consumer.frame_ready)
        self.assertEqual(info.source, self.backend.alias)

    async def test_begin_returns_accepted_answer_before_frame(self):
        self.backend.auto_frame = False
        info = await self.consumer.begin("camera.test")
        self.assertIsInstance(info, module.OpenedCamera)
        self.assertIs(module.ConsumerInfo, module.OpenedCamera)
        self.assertEqual(len(self.backend.peers[0].remote), 1)
        self.assertFalse(self.consumer.frame_ready)
        self.assertTrue(self.consumer.active)
        self.assertEqual(info.source, self.backend.alias)
        pending = asyncio.create_task(self.consumer.wait_frame())
        await asyncio.sleep(0)
        self.assertFalse(pending.done())
        self.backend.peers[0].video.queue.put_nowait("first-real-frame")
        self.assertEqual(await pending, info)
        self.assertTrue(self.consumer.frame_ready)

    async def test_begin_never_returns_before_remote_answer_accepted(self):
        self.backend.camera.messages = []
        task = asyncio.create_task(self.consumer.begin("camera.test"))
        await self.backend.camera.offer_entered.wait()
        gate = self.backend.peers[0].remote_gate = asyncio.Event()
        self.backend.camera.callback({"type": "answer", "answer": "v=0"})
        await asyncio.sleep(.01)
        self.assertFalse(task.done())
        self.assertFalse(self.consumer.frame_ready)
        gate.set()
        await task

    async def test_begin_preanswer_error_auto_cleans_without_handoff(self):
        self.backend.camera.messages = [{"type": "error", "message": "private"}]
        with self.assertRaises(ConsumerError) as caught:
            await self.consumer.begin("camera.test")
        self.assertEqual(caught.exception.code, "provider_error")
        self.assertTrue(self.consumer.cleanup_ok)
        self.assertEqual(len(self.backend.camera.closed_ids), 1)

    async def test_cancel_begin_before_answer_auto_cleans(self):
        self.backend.camera.messages = []
        task = asyncio.create_task(self.consumer.begin("camera.test"))
        await self.backend.camera.offer_entered.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError): await task
        self.assertFalse(self.consumer.active)
        self.assertEqual(len(self.backend.camera.closed_ids), 1)

    async def test_close_while_begin_pending_preserves_other_session(self):
        self.backend.camera.messages = []
        task = asyncio.create_task(self.consumer.begin("camera.test"))
        await self.backend.camera.offer_entered.wait()
        await self.consumer.close()
        with self.assertRaises(ConsumerError) as caught: await task
        self.assertEqual(caught.exception.code, "consumer_closed")
        self.assertEqual(self.backend.camera.sessions, {"other-ui-session"})

    async def test_wait_frame_without_begin_rejected(self):
        with self.assertRaises(ConsumerError) as caught: await self.consumer.wait_frame()
        self.assertEqual(caught.exception.code, "consumer_not_started")

    async def test_cancel_wait_frame_retains_reference_until_explicit_close(self):
        self.backend.auto_frame = False
        info = await self.consumer.begin("camera.test")
        pending = asyncio.create_task(self.consumer.wait_frame())
        await asyncio.sleep(0)
        pending.cancel()
        with self.assertRaises(asyncio.CancelledError): await pending
        self.assertEqual(await self.consumer.wait_failure(), "open_cancelled")
        self.assertEqual(self.backend.camera.closed_ids, [])
        self.assertIn(info.session_id, self.backend.camera.sessions)
        self.assertEqual(self.backend.peers[0].close_count, 0)
        self.assertFalse(self.consumer.frame_ready)
        self.assertFalse(self.consumer.active)
        await self.consumer.close()
        self.assertEqual(self.backend.camera.closed_ids, [info.session_id])
        self.assertTrue(self.consumer.cleanup_ok)

    async def test_wait_frame_error_retains_reference_and_never_ready(self):
        self.backend.auto_frame = False
        info = await self.consumer.begin("camera.test")
        self.backend.camera.callback({"type": "error", "message": "private-url"})
        self.assertEqual(await self.consumer.wait_failure(), "provider_error")
        with self.assertRaises(ConsumerError) as caught: await self.consumer.wait_frame()
        self.assertEqual(caught.exception.code, "provider_error")
        self.assertFalse(self.consumer.frame_ready)
        self.assertEqual(self.backend.camera.closed_ids, [])
        self.assertEqual(self.backend.peers[0].close_count, 0)
        await self.consumer.close()
        self.assertEqual(self.backend.camera.closed_ids, [info.session_id])

    async def test_begin_startup_timeout_without_waiter_retains_reference(self):
        self.backend.auto_frame = False
        # Leave negotiation enough headroom for Windows' 15.625ms loop clock.
        # This case tests expiry AFTER begin without a frame waiter, not whether
        # the fake peer can negotiate inside a sub-resolution startup budget.
        self.consumer = Consumer(None, backend=self.backend, open_timeout=.15, frame_timeout=1)
        info = await self.consumer.begin("camera.test")
        self.assertEqual(await asyncio.wait_for(self.consumer.wait_failure(), .5), "open_timeout")
        self.assertEqual(self.backend.camera.closed_ids, [])
        self.assertEqual(self.backend.peers[0].close_count, 0)
        self.assertFalse(self.consumer.frame_ready)
        closed = asyncio.create_task(self.consumer.wait_closed())
        await asyncio.sleep(0)
        self.assertFalse(closed.done())
        await self.consumer.close()
        await closed
        self.assertEqual(self.backend.camera.closed_ids, [info.session_id])

    async def test_begin_and_frame_share_single_total_timeout(self):
        self.backend.auto_frame = False
        self.backend.camera.answer_gate = asyncio.Event()
        self.consumer = Consumer(None, backend=self.backend, open_timeout=.3, frame_timeout=1)
        start = asyncio.get_running_loop().time()
        with mock.patch.object(module.asyncio, "timeout_at", wraps=asyncio.timeout_at) as timeouts:
            begun = asyncio.create_task(self.consumer.begin("camera.test"))
            await self.backend.camera.offer_entered.wait()
            await asyncio.sleep(.05)
            self.backend.camera.answer_gate.set()
            await begun
            deadline = self.consumer._session.open_deadline
            with self.assertRaises(ConsumerError) as caught: await self.consumer.wait_frame()
        self.assertEqual([call.args[0] for call in timeouts.call_args_list], [deadline, deadline],
                         "Both readiness gates must use one absolute deadline")
        self.assertEqual(caught.exception.code, "open_timeout")
        self.assertLess(asyncio.get_running_loop().time() - start, .6)
        self.assertEqual(self.consumer._session.open_deadline, deadline)
        self.assertEqual(self.backend.camera.closed_ids, [])

    async def test_close_wait_frame_rejects_ready_and_closes_once(self):
        self.backend.auto_frame = False
        info = await self.consumer.begin("camera.test")
        waiter = asyncio.create_task(self.consumer.wait_frame())
        await asyncio.sleep(0)
        await asyncio.gather(*(self.consumer.close() for _ in range(4)))
        with self.assertRaises(ConsumerError) as caught: await waiter
        self.assertEqual(caught.exception.code, "consumer_closed")
        self.assertEqual(self.backend.camera.closed_ids, [info.session_id])
        self.assertEqual(self.backend.peers[0].close_count, 1)

    async def test_managed_failure_after_frame_waits_for_reap_permission(self):
        info = await self.consumer.begin("camera.test")
        await self.consumer.wait_frame()
        self.backend.peers[0].video.queue.put_nowait(RuntimeError("private"))
        self.assertEqual(await asyncio.wait_for(self.consumer.wait_failure(), .2), "media_failed")
        self.assertEqual(self.backend.camera.closed_ids, [])
        self.assertEqual(self.backend.peers[0].close_count, 0)
        self.assertFalse(self.consumer.frame_ready)
        with self.assertRaises(ConsumerError): await self.consumer.wait_frame()
        await self.consumer.close()
        self.assertEqual(self.backend.camera.closed_ids, [info.session_id])

    async def test_managed_busy_until_explicit_release_even_after_failure(self):
        self.backend.auto_frame = False
        await self.consumer.begin("camera.test")
        self.backend.camera.callback({"type": "closed"})
        await self.consumer.wait_failure()
        with self.assertRaises(ConsumerError) as caught: await self.consumer.begin("camera.test")
        self.assertEqual(caught.exception.code, "consumer_busy")
        self.assertEqual(len(self.backend.peers), 1)
        await self.consumer.close()
        self.backend.auto_frame = True
        await self.consumer.begin("camera.test")
        await self.consumer.wait_frame()
        self.assertEqual(len(self.backend.peers), 2)

    async def test_late_wait_failure_from_previous_generation_does_not_follow_new(self):
        first = await self.consumer.begin("camera.test")
        watcher = asyncio.create_task(self.consumer.wait_failure())
        await asyncio.sleep(0)
        await self.consumer.close()
        second = await self.consumer.begin("camera.test")
        self.assertIsNone(await watcher)
        self.assertNotEqual(first.session_id, second.session_id)
        self.assertTrue(self.consumer.active)

    async def test_receive_only_audio_video_no_microphone(self):
        await self.opened()
        self.assertEqual(self.backend.peers[0].transceivers,
                         [("video", "recvonly"), ("audio", "recvonly")])

    async def test_optional_audio_false(self):
        self.consumer = Consumer(None, backend=self.backend, receive_audio=False)
        await self.opened()
        self.assertEqual(self.backend.peers[0].transceivers, [("video", "recvonly")])

    async def test_scoped_close_preserves_foreign_ui_reference(self):
        info = await self.opened()
        await self.consumer.close()
        self.assertEqual(self.backend.camera.closed_ids, [info.session_id])
        self.assertEqual(self.backend.camera.sessions, {"other-ui-session"})
        self.assertEqual(self.backend.peers[0].close_count, 1)

    async def test_concurrent_close_is_idempotent(self):
        await self.opened()
        await asyncio.gather(*(self.consumer.close() for _ in range(8)))
        self.assertEqual(len(self.backend.camera.closed_ids), 1)
        self.assertEqual(self.backend.peers[0].close_count, 1)

    async def test_close_before_runner_first_instruction(self):
        task = asyncio.create_task(self.opened())
        await asyncio.sleep(0)
        await self.consumer.close()
        with self.assertRaises(ConsumerError):
            await task
        self.assertFalse(self.consumer.active)
        self.assertEqual(self.backend.camera.calls, [])

    async def test_busy_does_not_replace_reference(self):
        info = await self.opened()
        await self.expect_error("consumer_busy")
        self.assertEqual(self.consumer.session_id, info.session_id)
        self.assertEqual(len(self.backend.camera.calls), 1)

    async def test_no_source_for_generic_frame_callback_mode(self):
        self.consumer = Consumer(None, backend=self.backend)
        info = await self.opened()
        self.assertIsNone(info.source)
        self.assertTrue(self.consumer.frame_ready)

    async def test_native_url_is_rejected(self):
        self.backend.alias = "rtsp://user:password@192.0.2.20:6554/doorbell"
        await self.expect_error("invalid_local_alias")
        self.assertEqual(self.backend.peers, [])

    async def test_source_query_and_fragment_rejected(self):
        for tail in ("alias?password=x", "alias#video=x", "../camera", "alias/extra", "alias%GG"):
            self.backend.alias = "rtsp://127.0.0.1:18554/" + tail
            await self.expect_error("invalid_local_alias")

    async def test_off_camera_is_not_turned_on(self):
        self.backend.camera.is_on = False
        await self.expect_error("camera_unavailable")
        self.assertEqual(self.backend.camera.calls, [])

    async def test_unavailable_camera_no_offer(self):
        self.backend.camera.available = False
        await self.expect_error("camera_unavailable")

    async def test_unsupported_camera_no_offer(self):
        self.backend.camera.supported = False
        await self.expect_error("webrtc_unsupported")

    async def test_resolver_error_sanitized(self):
        self.backend.resolve_failure = True
        await self.expect_error("camera_unavailable")

    async def test_invalid_entity_id(self):
        for entity_id in ("button.test", "camera.test?token=secret", "camera.Test", "", None):
            with self.assertRaises(ConsumerError) as caught:
                await self.consumer.open(entity_id)
            self.assertEqual(caught.exception.code, "invalid_entity_id")

    async def test_candidates_before_answer_wait_for_answer(self):
        self.backend.camera.messages = [
            {"type": "candidate", "candidate": {"candidate": "candidate:x", "sdpMid": "0"}},
            {"type": "answer", "answer": "v=0"},
            {"type": "candidate", "candidate": {"candidate": ""}},
        ]
        await self.opened()
        self.assertEqual(len(self.backend.peers[0].candidates), 2)

    async def test_invalid_candidate_index_bool(self):
        self.backend.camera.messages = [{"type": "candidate", "candidate":
                                        {"candidate": "x", "sdpMLineIndex": True}}]
        await self.expect_error("invalid_candidate")

    async def test_candidate_size_and_shape_fail_closed(self):
        for candidate in (None, "private", {"candidate": "x" * 4097},
                          {"candidate": "x", "sdpMid": 2}):
            self.backend.camera.messages = [{"type": "candidate", "candidate": candidate}]
            await self.expect_error("invalid_candidate")

    async def test_candidate_limit(self):
        self.backend.camera.messages = [
            {"type": "candidate", "candidate": {"candidate": "x"}} for _ in range(65)]
        await self.expect_error("candidate_limit")

    async def test_signal_queue_bounded(self):
        self.backend.camera.messages = [
            {"type": "candidate", "candidate": {"candidate": "x"}} for _ in range(129)]
        await self.expect_error("signal_limit")

    async def test_provider_error_sanitized(self):
        self.backend.camera.messages = [{"type": "error", "code": "private", "message": "rtsp://secret"}]
        await self.expect_error("provider_error")
        self.assertEqual(len(self.backend.camera.closed_ids), 1)

    async def test_duplicate_answer_fails_closed(self):
        self.backend.camera.messages *= 2
        await self.expect_error("invalid_answer")

    async def test_invalid_answer_fails_closed(self):
        for answer in (None, "", 2, "x" * (256 * 1024 + 1)):
            self.backend.camera.messages = [{"type": "answer", "answer": answer}]
            await self.expect_error("invalid_answer")

    async def test_wrong_session_message(self):
        self.backend.camera.messages = [{"type": "session", "session_id": "foreign"}]
        await self.expect_error("invalid_signal")

    async def test_protocol_object_callback_accepted(self):
        class Answer:
            def as_dict(self):
                return {"type": "answer", "answer": "v=0", "ignored": "private"}
        self.backend.camera.messages = [Answer()]
        await self.opened()
        self.assertTrue(self.consumer.frame_ready)

    async def test_unknown_signal_fails_closed(self):
        self.backend.camera.messages = [{"type": "foreign", "value": "private"}]
        await self.expect_error("invalid_signal")

    async def test_no_answer_times_out_and_closes_owned_session(self):
        self.backend.camera.messages = []
        await self.expect_error("open_timeout")
        self.assertEqual(len(self.backend.camera.closed_ids), 1)
        self.assertFalse(self.consumer.active)

    async def test_cancel_open_cleanup(self):
        self.backend.camera.messages = []
        task = asyncio.create_task(self.opened())
        await self.backend.camera.offer_entered.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertFalse(self.consumer.active)
        self.assertEqual(len(self.backend.camera.closed_ids), 1)

    async def test_hanging_provider_is_canceled_scoped_after_first_frame(self):
        self.backend.camera.hang = True
        info = await self.opened()
        await self.consumer.close()
        self.assertEqual(self.backend.camera.closed_ids, [info.session_id])

    async def test_old_callback_cannot_close_new_session(self):
        old = await self.opened()
        callback = self.backend.camera.callback
        await self.consumer.close()
        new = await self.opened()
        callback({"type": "closed", "reason": "late"})
        callback({"type": "answer", "answer": "late"})
        await asyncio.sleep(0)
        self.assertNotEqual(old.session_id, new.session_id)
        self.assertTrue(self.consumer.active)
        self.assertIsNone(self.consumer.failure_code)

    async def test_video_track_end_auto_cleanup(self):
        await self.opened()
        self.backend.peers[0].video.queue.put_nowait(RuntimeError("private"))
        await asyncio.wait_for(self.consumer.wait_closed(), .2)
        self.assertEqual(self.consumer.failure_code, "media_failed")

    async def test_video_stall_watchdog(self):
        self.consumer = Consumer(None, backend=self.backend, frame_timeout=.025)
        await self.opened()
        await asyncio.wait_for(self.consumer.wait_closed(), .2)
        self.assertEqual(self.consumer.failure_code, "media_timeout")

    async def test_optional_frame_callback_and_audio_drained(self):
        frames = []
        self.consumer = Consumer(None, backend=self.backend, on_frame=frames.append)
        await self.opened()
        await asyncio.sleep(0)
        self.assertEqual(frames, ["video-frame"])
        self.assertEqual(self.backend.peers[0].audio.queue.qsize(), 0)

    async def test_awaitable_callback_serialized(self):
        frames = []
        async def sink(frame):
            await asyncio.sleep(0)
            frames.append(frame)
        self.consumer = Consumer(None, backend=self.backend, on_frame=sink)
        await self.opened()
        self.assertEqual(frames, ["video-frame"])

    async def test_callback_failure_prevents_ready(self):
        def sink(frame):
            raise RuntimeError("sensitive")
        self.consumer = Consumer(None, backend=self.backend, on_frame=sink)
        await self.expect_error("media_failed")

    async def test_callback_timeout(self):
        async def sink(frame):
            await asyncio.Event().wait()
        self.consumer = Consumer(None, backend=self.backend, on_frame=sink, callback_timeout=.015)
        await self.expect_error("media_timeout")

    async def test_camera_close_failure_sanitized(self):
        await self.opened()
        self.backend.camera.close_failure = True
        await self.consumer.close()
        self.assertEqual(self.consumer.failure_code, "camera_close_failed")

    async def test_peer_close_failure_does_not_skip_camera_close(self):
        info = await self.opened()
        self.backend.peers[0].close_failure = True
        await self.consumer.close()
        self.assertEqual(self.backend.camera.closed_ids, [info.session_id])
        self.assertEqual(self.consumer.failure_code, "peer_close_failed")

    async def test_connection_failure_cleanup(self):
        await self.opened()
        peer = self.backend.peers[0]
        peer.connectionState = "failed"
        peer.handlers["connectionstatechange"]()
        await asyncio.wait_for(self.consumer.wait_closed(), .2)
        self.assertEqual(self.consumer.failure_code, "peer_closed")

    async def test_unconfirmed_peer_cleanup_prevents_reopen(self):
        await self.opened()
        self.backend.peers[0].close_failure = True
        await self.consumer.close()
        self.assertFalse(self.consumer.cleanup_ok)
        await self.expect_error("consumer_cleanup_unconfirmed")
        self.assertEqual(len(self.backend.peers), 1)

    async def test_unconfirmed_scoped_camera_cleanup_prevents_reopen(self):
        await self.opened()
        self.backend.camera.close_failure = True
        await self.consumer.close()
        self.assertFalse(self.consumer.cleanup_ok)
        await self.expect_error("consumer_cleanup_unconfirmed")

    async def test_successful_local_cleanup_allows_new_generation(self):
        old = await self.opened()
        await self.consumer.close()
        self.assertTrue(self.consumer.cleanup_ok)
        new = await self.opened()
        self.assertNotEqual(old.session_id, new.session_id)

    def test_invalid_timeouts(self):
        for value in (0, -1, True, float("nan"), float("inf"), "30"):
            with self.assertRaises(ValueError):
                Consumer(None, backend=self.backend, open_timeout=value)


if __name__ == "__main__":
    unittest.main(verbosity=2)
