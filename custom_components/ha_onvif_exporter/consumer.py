"""A real, receive-only consumer of the public Home Assistant camera API.

No native-camera API, camera.stream_source(), private session maps, service calls,
or HA credentials are used. Lifetime (including the 30 s export window) belongs
to the caller. Close an external RTSP reader BEFORE closing this reference.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
import inspect
import math
import re
import secrets
from typing import Any, Protocol

_ENTITY_ID = re.compile(r"camera\.[a-z0-9_]+\Z")
_ALIAS = re.compile(r"(?:[A-Za-z0-9._-]|%[0-9A-Fa-f]{2}){1,1024}\Z")
_MAX_SDP = 256 * 1024
_MAX_CANDIDATE = 4096
_MAX_SIGNALS = 128
_MAX_CANDIDATES = 64


class ConsumerError(Exception):
    """Sanitized failure; never include SDP, ICE credentials, or camera URLs."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True, slots=True)
class OpenedCamera:
    """Public metadata. Source is the HA loopback alias, never the camera URL."""

    entity_id: str
    session_id: str
    source: str | None


# Preserve the original metadata name for existing callers.
ConsumerInfo = OpenedCamera


class ConsumerBackend(Protocol):
    """Injectable boundary for deterministic, offline tests."""

    def resolve_camera(self, hass: Any, entity_id: str) -> Any: ...
    def create_peer(self, camera: Any) -> Any: ...
    def description(self, sdp: str) -> Any: ...
    def candidate(self, data: Mapping[str, Any]) -> Any: ...
    def source(self, camera: Any) -> str: ...
    def supports_webrtc(self, camera: Any) -> bool: ...


class HomeAssistantBackend:
    """HA 2026.8.1 camera API and aiortc 1.15.x adapter, imported on demand."""

    def resolve_camera(self, hass: Any, entity_id: str) -> Any:
        from homeassistant.components.camera.helper import get_camera_from_entity_id

        return get_camera_from_entity_id(hass, entity_id)

    def supports_webrtc(self, camera: Any) -> bool:
        from homeassistant.components.camera.const import StreamType

        return StreamType.WEB_RTC in camera.camera_capabilities.frontend_stream_types

    def create_peer(self, camera: Any) -> Any:
        from aiortc import RTCConfiguration, RTCIceServer, RTCPeerConnection

        config = camera.async_get_webrtc_client_configuration()
        if config.data_channel is not None:
            # Initial exporter supports HA/go2rtc media, not camera-specific
            # data-channel protocols. Reject before making the camera offer.
            raise ConsumerError("data_channel_unsupported")
        servers = [
            RTCIceServer(
                urls=server.urls,
                username=server.username,
                credential=server.credential,
            )
            for server in config.configuration.ice_servers
        ]
        # setLocalDescription gathers ICE before we send the complete offer.
        # No microphone, media sender, browser, or authenticated WS is required.
        return RTCPeerConnection(configuration=RTCConfiguration(iceServers=servers))

    def description(self, sdp: str) -> Any:
        from aiortc import RTCSessionDescription

        return RTCSessionDescription(sdp=sdp, type="answer")

    def candidate(self, data: Mapping[str, Any]) -> Any:
        from aiortc.sdp import candidate_from_sdp

        text = data["candidate"]
        if text == "":
            return None  # end-of-candidates, supported by aiortc
        candidate = candidate_from_sdp(text.removeprefix("candidate:"))
        candidate.sdpMid = data.get("sdpMid")
        candidate.sdpMLineIndex = data.get("sdpMLineIndex")
        # go2rtc's HA adapter supplies bare candidate strings. Its offer/answer
        # uses BUNDLE: route an unqualified candidate to our first video m-line.
        if candidate.sdpMid is None and candidate.sdpMLineIndex is None:
            candidate.sdpMLineIndex = 0
        return candidate

    def source(self, camera: Any) -> str:
        from homeassistant.components.go2rtc.util import get_camera_identifier

        alias = get_camera_identifier(camera)
        if not isinstance(alias, str) or not _ALIAS.fullmatch(alias):
            raise ConsumerError("invalid_local_alias")
        return f"rtsp://127.0.0.1:18554/{alias}"


@dataclass(slots=True)
class _Session:
    info: OpenedCamera
    camera: Any
    negotiated: asyncio.Future[OpenedCamera]
    ready: asyncio.Future[OpenedCamera]
    open_deadline: float
    stopped: asyncio.Event = field(default_factory=asyncio.Event)
    closed: asyncio.Event = field(default_factory=asyncio.Event)
    signals: asyncio.Queue[Mapping[str, Any]] = field(
        default_factory=lambda: asyncio.Queue(maxsize=_MAX_SIGNALS)
    )
    tasks: set[asyncio.Task[Any]] = field(default_factory=set)
    runner: asyncio.Task[None] | None = None
    started: asyncio.Event = field(default_factory=asyncio.Event)
    phase: str = "new"
    cancel_requested: bool = False
    peer: Any = None
    offer_attempted: bool = False
    closing: bool = False
    frame_received: bool = False
    failure: str | None = None
    video_tracks: int = 0
    audio_tracks: int = 0
    cleanup_ok: bool = True
    managed_lifetime: bool = False
    release_requested: asyncio.Event = field(default_factory=asyncio.Event)
    startup_timer: asyncio.TimerHandle | None = None


class HACameraConsumer:
    """Hold exactly one owned HA WebRTC session and continuously drain its tracks.

    ``managed_go2rtc=True`` is an explicit deployment prerequisite, not automatic
    detection. It supplies the fixed loopback alias for the caller's RTSP worker;
    a native WebRTC-only camera may not provide such an alias. Generic consumers
    can use on_frame and leave this false. Neither mode reads a native stream URL.

    on_frame must be fast/nonblocking. An awaitable return is serialized and has
    a bounded timeout. The module never queues an unbounded backlog of frames.
    """

    def __init__(
        self,
        hass: Any,
        *,
        on_frame: Callable[[Any], Any] | None = None,
        managed_go2rtc: bool = False,
        receive_audio: bool = True,
        open_timeout: float = 15.0,
        frame_timeout: float = 10.0,
        callback_timeout: float = 1.0,
        close_timeout: float = 5.0,
        backend: ConsumerBackend | None = None,
    ) -> None:
        for value in (open_timeout, frame_timeout, callback_timeout, close_timeout):
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise ValueError("invalid_timeout")
        if type(managed_go2rtc) is not bool or type(receive_audio) is not bool:
            raise ValueError("invalid_option")
        self._hass = hass
        self._backend = backend or HomeAssistantBackend()
        self._on_frame = on_frame
        self._managed_go2rtc = managed_go2rtc
        self._receive_audio = receive_audio
        self._open_timeout = open_timeout
        self._frame_timeout = frame_timeout
        self._callback_timeout = callback_timeout
        self._close_timeout = close_timeout
        self._lock = asyncio.Lock()
        self._session: _Session | None = None

    @property
    def source(self) -> str | None:
        return self._session.info.source if self._session else None

    @property
    def session_id(self) -> str | None:
        return self._session.info.session_id if self._session else None

    @property
    def frame_ready(self) -> bool:
        return bool(self._session and self._session.frame_received and self.active)

    @property
    def active(self) -> bool:
        return bool(self._session and not self._session.closing and not self._session.closed.is_set()
                    and not self._session.stopped.is_set())

    @property
    def failure_code(self) -> str | None:
        return self._session.failure if self._session else None

    @property
    def cleanup_ok(self) -> bool:
        """Local peer/tasks and scoped callback succeeded, not native idle ACK."""
        return bool(self._session and self._session.closed.is_set() and self._session.cleanup_ok)

    async def begin(self, entity_id: str) -> OpenedCamera:
        """Negotiate an owned session; no claim of a received frame is made.

        Once returned, the caller MUST explicitly close after reaping its reader.
        Failure/cancellation of wait_frame never releases this HA reference.
        """
        session, info = await self._begin(entity_id)
        # No await between handing off this lifetime and returning its metadata.
        session.managed_lifetime = True
        return info

    async def open(self, entity_id: str) -> OpenedCamera:
        """Legacy frame-ready open with automatic cleanup on failure/cancel."""
        session, _ = await self._begin(entity_id)
        return await self._wait_gate(session, session.ready)

    async def _begin(self, entity_id: str) -> tuple[_Session, OpenedCamera]:
        if not isinstance(entity_id, str) or not _ENTITY_ID.fullmatch(entity_id):
            raise ConsumerError("invalid_entity_id")
        async with self._lock:
            if self._session and not self._session.closed.is_set():
                raise ConsumerError("consumer_busy")
            if self._session and not self._session.cleanup_ok:
                raise ConsumerError("consumer_cleanup_unconfirmed")
            try:
                camera = self._backend.resolve_camera(self._hass, entity_id)
                if not camera.available or not camera.is_on:
                    raise ConsumerError("camera_unavailable")
                if not self._backend.supports_webrtc(camera):
                    raise ConsumerError("webrtc_unsupported")
                source = self._backend.source(camera) if self._managed_go2rtc else None
                if source is not None and not self._valid_source(source):
                    raise ConsumerError("invalid_local_alias")
            except ConsumerError:
                raise
            except Exception:
                raise ConsumerError("camera_unavailable") from None
            loop = asyncio.get_running_loop()
            info = OpenedCamera(entity_id, secrets.token_hex(16), source)
            session = self._session = _Session(
                info, camera, loop.create_future(), loop.create_future(),
                loop.time() + self._open_timeout,
            )
            # Failure may be reported through wait_failure without either gate
            # being awaited; retrieving it here does not change await semantics.
            for gate in (session.negotiated, session.ready):
                gate.add_done_callback(lambda future: None if future.cancelled() else future.exception())
            session.startup_timer = loop.call_at(session.open_deadline, self._startup_expired, session)
            session.runner = asyncio.create_task(self._run(session))
        info = await self._wait_gate(session, session.negotiated)
        return session, info

    async def wait_frame(self) -> OpenedCamera:
        """Wait for this generation's actual video under its original deadline."""
        session = self._session
        if session is None:
            raise ConsumerError("consumer_not_started")
        return await self._wait_gate(session, session.ready)

    async def _wait_gate(self, session: _Session, gate: asyncio.Future[OpenedCamera]) -> OpenedCamera:
        try:
            async with asyncio.timeout_at(session.open_deadline):
                info = await asyncio.shield(gate)
            if session.closing or session.closed.is_set() or session.stopped.is_set():
                raise ConsumerError(session.failure or "consumer_closed")
            return info
        except TimeoutError:
            self._fail(session, "open_timeout")
            if not session.managed_lifetime:
                await self._close_session(session)
            raise ConsumerError("open_timeout") from None
        except asyncio.CancelledError:
            if session.managed_lifetime:
                self._fail(session, "open_cancelled")
            else:
                await self._close_session(session)
            raise
        except ConsumerError:
            if not session.managed_lifetime:
                await self._close_session(session)
            raise

    def _startup_expired(self, session: _Session) -> None:
        session.startup_timer = None
        self._fail(session, "open_timeout")

    @staticmethod
    def _cancel_startup_timer(session: _Session) -> None:
        if session.startup_timer is not None:
            session.startup_timer.cancel()
            session.startup_timer = None

    async def close(self) -> None:
        """Close only our own peer and public camera subscription.

        The public HA callback can schedule asynchronous cleanup; its return is
        NOT an acknowledgment that the camera's upstream/physical bus is idle.
        """
        async with self._lock:
            session = self._session
        if session:
            await self._close_session(session)

    async def wait_closed(self) -> None:
        """Wait for this consumer's cleanup (use failure_code to distinguish it)."""
        session = self._session
        if session:
            await session.closed.wait()

    async def wait_failure(self) -> str | None:
        """Signal failure/stop without releasing a managed session reference."""
        session = self._session
        if session:
            await session.stopped.wait()
            return session.failure
        return None

    @staticmethod
    def _valid_source(source: str) -> bool:
        prefix = "rtsp://127.0.0.1:18554/"
        return source.startswith(prefix) and bool(_ALIAS.fullmatch(source[len(prefix):]))

    def _fail(self, session: _Session, code: str) -> None:
        if session.closing or session.closed.is_set():
            return
        if session.failure is None:
            session.failure = code
        if not session.negotiated.done():
            session.negotiated.set_exception(ConsumerError(session.failure))
        if not session.ready.done():
            session.ready.set_exception(ConsumerError(session.failure))
        session.stopped.set()
        # A provider may still be awaiting unrelated work after its answer.
        # Enter the release fence promptly, but never cancel cleanup twice.
        if session.managed_lifetime and session.phase == "running" and not session.cancel_requested:
            if session.runner and session.runner is not asyncio.current_task() and not session.runner.done():
                session.cancel_requested = True
                session.runner.cancel()

    async def _close_session(self, session: _Session) -> None:
        # Never follow self._session after an await: a previous close must not
        # affect a subsequent generation. A canceled caller cannot cancel cleanup.
        if not session.closed.is_set():
            session.release_requested.set()
            session.closing = True
            session.stopped.set()
            self._cancel_startup_timer(session)
            if not session.negotiated.done():
                session.negotiated.set_exception(ConsumerError("consumer_closed"))
            if not session.ready.done():
                session.ready.set_exception(ConsumerError("consumer_closed"))
            # A task canceled before its first instruction cannot run finally.
            # Let the runner enter its cleanup-protected scope first. It sees
            # closing and skips all camera/peer activity in this case.
            await asyncio.shield(session.started.wait())
            if session.phase == "running" and not session.cancel_requested:
                session.cancel_requested = True
                if session.runner and not session.runner.done():
                    session.runner.cancel()
        if session.runner:
            await asyncio.shield(session.runner)

    def _spawn(self, session: _Session, coroutine: Any) -> None:
        task = asyncio.create_task(coroutine)
        session.tasks.add(task)
        task.add_done_callback(session.tasks.discard)

    async def _run(self, session: _Session) -> None:
        session.phase = "running"
        session.started.set()
        try:
            if session.closing:
                return
            peer = session.peer = self._backend.create_peer(session.camera)
            peer.addTransceiver("video", direction="recvonly")
            if self._receive_audio:
                peer.addTransceiver("audio", direction="recvonly")

            @peer.on("track")
            def on_track(track: Any) -> None:
                if session.closing or session.closed.is_set() or session.stopped.is_set():
                    return
                if track.kind == "video":
                    session.video_tracks += 1
                    if session.video_tracks > 1:
                        self._fail(session, "unexpected_media")
                        return
                elif track.kind == "audio" and self._receive_audio:
                    session.audio_tracks += 1
                    if session.audio_tracks > 1:
                        self._fail(session, "unexpected_media")
                        return
                else:
                    self._fail(session, "unexpected_media")
                    return
                self._spawn(session, self._drain_track(session, track))

            @peer.on("connectionstatechange")
            def on_connection() -> None:
                if peer.connectionState in {"failed", "closed"}:
                    self._fail(session, "peer_closed")

            self._spawn(session, self._process_signals(session))
            offer = await peer.createOffer()
            await peer.setLocalDescription(offer)
            if session.closing or session.stopped.is_set():
                return
            # aiortc has gathered all local candidates into the SDP. Sending the
            # complete offer avoids creating unowned trickle-candidate tasks.
            sdp = peer.localDescription.sdp
            if not isinstance(sdp, str) or len(sdp) > _MAX_SDP:
                raise ConsumerError("invalid_offer")
            session.offer_attempted = True
            await session.camera.async_handle_async_webrtc_offer(
                sdp, session.info.session_id, lambda message: self._signal(session, message)
            )
            await session.stopped.wait()
        except asyncio.CancelledError:
            pass
        except ConsumerError as error:
            self._fail(session, error.code)
        except Exception:
            self._fail(session, "negotiation_failed")
        finally:
            if session.managed_lifetime and not session.release_requested.is_set():
                # The caller may still own an RTSP publisher. It must reap that
                # exact reader before permitting peer/public HA callback cleanup.
                session.phase = "awaiting_release"
                await asyncio.shield(session.release_requested.wait())
            await self._cleanup(session)

    def _signal(self, session: _Session, message: Any) -> None:
        if session.closing or session.closed.is_set():
            return
        try:
            if not isinstance(message, Mapping):
                message = message.as_dict()
            if not isinstance(message, Mapping):
                raise ValueError
            kind = message.get("type")
            if kind in {"error", "closed"}:
                self._fail(session, "provider_closed" if kind == "closed" else "provider_error")
                return
            if kind == "session":
                if message.get("session_id") != session.info.session_id:
                    raise ValueError
                return
            if kind not in {"answer", "candidate"}:
                raise ValueError
            if kind == "answer":
                answer = message.get("answer")
                if not isinstance(answer, str) or not answer or len(answer) > _MAX_SDP:
                    raise ConsumerError("invalid_answer")
                clean = {"type": "answer", "answer": answer}
            else:
                clean = {"type": "candidate", "candidate": self._candidate_data(message.get("candidate"))}
            # Store only bounded protocol fields, never arbitrary provider data.
            session.signals.put_nowait(clean)
        except asyncio.QueueFull:
            self._fail(session, "signal_limit")
        except ConsumerError as error:
            self._fail(session, error.code)
        except Exception:
            self._fail(session, "invalid_signal")

    async def _process_signals(self, session: _Session) -> None:
        answered = False
        candidates: list[Mapping[str, Any]] = []
        count = 0
        try:
            while not session.stopped.is_set():
                message = await session.signals.get()
                if message["type"] == "answer":
                    text = message.get("answer")
                    if answered or not isinstance(text, str) or not text or len(text) > _MAX_SDP:
                        raise ConsumerError("invalid_answer")
                    await session.peer.setRemoteDescription(self._backend.description(text))
                    if session.closing or session.stopped.is_set():
                        return
                    answered = True
                    if not session.negotiated.done():
                        session.negotiated.set_result(session.info)
                    if session.frame_received:
                        self._cancel_startup_timer(session)
                    for candidate in candidates:
                        await session.peer.addIceCandidate(self._backend.candidate(candidate))
                    candidates.clear()
                else:
                    candidate = self._candidate_data(message.get("candidate"))
                    count += 1
                    if count > _MAX_CANDIDATES:
                        raise ConsumerError("candidate_limit")
                    if answered:
                        await session.peer.addIceCandidate(self._backend.candidate(candidate))
                    else:
                        candidates.append(candidate)
        except asyncio.CancelledError:
            pass
        except ConsumerError as error:
            self._fail(session, error.code)
        except Exception:
            self._fail(session, "invalid_signal")

    @staticmethod
    def _candidate_data(value: Any) -> Mapping[str, Any]:
        if not isinstance(value, Mapping):
            converter = getattr(value, "to_dict", None)
            value = converter() if callable(converter) else None
        if not isinstance(value, Mapping):
            raise ConsumerError("invalid_candidate")
        text = value.get("candidate")
        mid = value.get("sdpMid")
        index = value.get("sdpMLineIndex")
        if not isinstance(text, str) or len(text) > _MAX_CANDIDATE:
            raise ConsumerError("invalid_candidate")
        if mid is not None and (not isinstance(mid, str) or len(mid) > 128):
            raise ConsumerError("invalid_candidate")
        if index is not None and (type(index) is not int or not 0 <= index <= 16):
            raise ConsumerError("invalid_candidate")
        return {"candidate": text, "sdpMid": mid, "sdpMLineIndex": index}

    async def _drain_track(self, session: _Session, track: Any) -> None:
        try:
            while not session.stopped.is_set():
                if track.kind == "video":
                    async with asyncio.timeout(self._frame_timeout):
                        frame = await track.recv()
                    if session.closing or session.stopped.is_set():
                        return
                    if self._on_frame is not None:
                        result = self._on_frame(frame)
                        if inspect.isawaitable(result):
                            async with asyncio.timeout(self._callback_timeout):
                                await result
                    if session.closing or session.stopped.is_set():
                        return
                    session.frame_received = True
                    if not session.ready.done():
                        session.ready.set_result(session.info)
                    if session.negotiated.done() and session.failure is None:
                        self._cancel_startup_timer(session)
                else:
                    await track.recv()  # Drain audio; never send microphone data.
        except asyncio.CancelledError:
            pass
        except TimeoutError:
            self._fail(session, "media_timeout")
        except Exception:
            self._fail(session, "media_failed")

    async def _cleanup(self, session: _Session) -> None:
        self._cancel_startup_timer(session)
        session.phase = "cleanup"
        session.closing = True
        session.stopped.set()
        tasks = tuple(session.tasks)
        for task in tasks:
            task.cancel()
        try:
            async with asyncio.timeout(self._close_timeout):
                await asyncio.gather(*tasks, return_exceptions=True)
        except TimeoutError:
            session.failure = session.failure or "track_close_failed"
            session.cleanup_ok = False
        try:
            if session.peer is not None:
                try:
                    async with asyncio.timeout(self._close_timeout):
                        await session.peer.close()
                except Exception:
                    session.failure = session.failure or "peer_close_failed"
                    session.cleanup_ok = False
            if session.offer_attempted:
                try:
                    # This is the same PUBLIC scoped callback used by HA when a
                    # frontend unsubscribes. It may schedule asynchronous cleanup.
                    session.camera.close_webrtc_session(session.info.session_id)
                except Exception:
                    session.failure = session.failure or "camera_close_failed"
                    session.cleanup_ok = False
        finally:
            if not session.negotiated.done():
                session.negotiated.set_exception(ConsumerError(session.failure or "consumer_closed"))
            if not session.ready.done():
                session.ready.set_exception(ConsumerError(session.failure or "consumer_closed"))
            # Consume exceptions even if opening was canceled before awaiting it.
            if not session.ready.cancelled():
                session.ready.exception()
            if not session.negotiated.cancelled():
                session.negotiated.exception()
            session.closed.set()
            session.phase = "closed"
