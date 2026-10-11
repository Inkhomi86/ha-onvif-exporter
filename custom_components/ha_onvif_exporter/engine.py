"""Owned media processes, private stock MediaMTX and an independent watchdog.

Only the owning process handles are signalled. No native camera/HA API, motion
decisions, door control, secret argv, payload logging or automatic retry exists.
The manager MUST confirm stop_video() before releasing its HA camera consumer.
"""
from __future__ import annotations

import asyncio
import base64
from dataclasses import dataclass, field
import hashlib
import io
from ipaddress import IPv4Address
import json
import math
import os
from pathlib import Path
import platform
import re
import secrets
import socket
import stat
import subprocess
import sys
import tarfile
import threading
import time
from typing import Any, Callable

ARCHIVE_SHA256 = "121be6e00397e473a95d83c75f4fcd3819a6af3b0e1a50889cba43ab4fab6a27"
BINARY_SHA256 = "0ffb7f18210d08ee38cb16668c4f3a2c96622e69d401119c0026d078a2b89523"
STANDBY_SHA256 = "fbdeec17613eac45ea6794b664286a57e268bb79ebef032ccb73911fe78387c8"
ARCHIVE_SIZE = 28345997
_COMPONENT = Path(__file__).resolve().parent
_SOURCE = re.compile(r"rtsp://127\.0\.0\.1:18554/(?:[A-Za-z0-9._-]|%[0-9A-Fa-f]{2}){1,1024}\Z")
_ENTRY = re.compile(r"[A-Za-z0-9_-]{1,64}\Z")
_USER = re.compile(r"[A-Za-z0-9_.-]{1,64}\Z")
_CALLBACK_CODES = frozenset({"expired", "worker_failed", "server_failed"})
_ENV = {"LANG": "C.UTF-8", "TZ": "UTC"}


class EngineError(Exception):
    """Non-sensitive, fixed error code; never wrap OS/libav/raw exceptions."""


def password_hash(value: str) -> str:
    return "sha256:" + base64.b64encode(hashlib.sha256(value.encode("ascii")).digest()).decode("ascii")


def validate_settings(settings: dict[str, Any]) -> dict[str, Any]:
    """Recheck security-critical settings, even when constructed outside the UI."""
    try:
        bind = IPv4Address(settings["bind_ip"])
        clients = settings["allowed_clients"]
        if str(bind) != settings["bind_ip"] or bind.is_unspecified or bind.is_multicast or str(bind) == "255.255.255.255":
            raise ValueError
        if not isinstance(clients, list) or not 1 <= len(clients) <= 32:
            raise ValueError
        parsed = []
        for value in clients:
            address = IPv4Address(value)
            if not isinstance(value, str) or str(address) != value or address.is_unspecified or address.is_multicast or str(address) == "255.255.255.255":
                raise ValueError
            if value not in parsed:
                parsed.append(value)
        port = settings["rtsp_port"]
        user, password = settings["username"], settings["password"]
        if type(port) is not int or not 1024 <= port <= 65535:
            raise ValueError
        if not isinstance(user, str) or not _USER.fullmatch(user) or user in {"any", "window_publisher"}:
            raise ValueError
        if not isinstance(password, str) or not 16 <= len(password) <= 128 or not password.isascii() or any(ord(char) < 32 or ord(char) == 127 for char in password):
            raise ValueError
        return {"bind_ip": str(bind), "rtsp_port": port, "username": user, "password": password, "allowed_clients": parsed}
    except Exception:
        raise EngineError("invalid_settings") from None


def media_config(settings: dict[str, Any], publisher_password: str, standby: Path) -> dict[str, Any]:
    """JSON is valid YAML; hashes only, exact roles/peers and one fixed path."""
    return {
        "logLevel": "error", "logDestinations": ["stdout"], "dumpPackets": False,
        "readTimeout": "5s", "writeTimeout": "5s", "writeQueueSize": 128,
        "authMethod": "internal",
        "authInternalUsers": [
            {"user": password_hash(settings["username"]), "pass": password_hash(settings["password"]),
             "ips": [ip + "/32" for ip in settings["allowed_clients"]], "permissions": [{"action": "read", "path": "standby"}]},
            {"user": password_hash("window_publisher"), "pass": password_hash(publisher_password),
             "ips": [settings["bind_ip"] + "/32"], "permissions": [{"action": "publish", "path": "standby"}]},
        ],
        "api": False, "metrics": False, "pprof": False, "playback": False,
        "rtsp": True, "rtspAddress": f"{settings['bind_ip']}:{settings['rtsp_port']}",
        "rtspTransports": ["tcp"], "rtspEncryption": "no", "rtspAuthMethods": ["basic"],
        "rtspTrustedProxies": [], "rtmp": False, "hls": False, "webrtc": False, "srt": False, "moq": False,
        "paths": {"standby": {"source": "publisher", "overridePublisher": False,
            "maxReaders": max(4, len(settings["allowed_clients"]) * 2), "record": False,
            "alwaysAvailable": True, "alwaysAvailableFile": str(standby), "alwaysAvailableRecorded": False}},
    }


def _private_directory(path: Path) -> None:
    try:
        path.mkdir(mode=0o700)
    except FileExistsError:
        pass
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != getattr(os, "geteuid", lambda: 0)() or stat.S_IMODE(info.st_mode) != 0o700:
        raise EngineError("runtime_permissions")


def _private_file(path: Path, content: bytes, mode: int) -> None:
    """Replace only our own exact private regular file; never follow symlinks."""
    try:
        previous = path.lstat()
    except FileNotFoundError:
        previous = None
    if previous is not None and (not stat.S_ISREG(previous.st_mode) or previous.st_uid != getattr(os, "geteuid", lambda: 0)() or stat.S_IMODE(previous.st_mode) != mode):
        raise EngineError("runtime_permissions")
    temporary = path.with_name(path.name + "." + secrets.token_hex(8))
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), mode)
    try:
        os.fchmod(fd, mode)
        written = 0
        while written < len(content):
            size = os.write(fd, content[written:])
            if size <= 0:
                raise EngineError("runtime_write")
            written += size
        os.fsync(fd)
        os.close(fd)
        fd = -1
        os.replace(temporary, path)
    finally:
        if fd >= 0:
            os.close(fd)
        if temporary.exists():
            temporary.unlink()


def _read_pinned(path: Path, digest: str, limit: int) -> bytes:
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(fd, "rb") as source:
        info = os.fstat(source.fileno())
        if not stat.S_ISREG(info.st_mode) or not 0 < info.st_size <= limit:
            raise EngineError("asset_invalid")
        content = source.read(limit + 1)
    if len(content) > limit or hashlib.sha256(content).hexdigest() != digest:
        raise EngineError("asset_invalid")
    return content


def _read_archive(assets: Path) -> bytes:
    """Eight small repository blobs reconstruct the pinned official archive."""
    pieces = []
    for index in range(8):
        fd = os.open(assets / f"mediamtx.tar.gz.part{index:02d}", os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        with os.fdopen(fd, "rb") as source:
            info = os.fstat(source.fileno())
            expected = 4000000 if index < 7 else ARCHIVE_SIZE - 7 * 4000000
            if not stat.S_ISREG(info.st_mode) or info.st_size != expected:
                raise EngineError("asset_invalid")
            content = source.read(expected + 1)
        if len(content) != expected:
            raise EngineError("asset_invalid")
        pieces.append(content)
    archive = b"".join(pieces)
    if hashlib.sha256(archive).hexdigest() != ARCHIVE_SHA256:
        raise EngineError("asset_invalid")
    return archive


@dataclass(eq=False)
class _Worker:
    ready: asyncio.Future[None]
    process: Any = None
    spawn_done: threading.Event = field(default_factory=threading.Event)
    spawn_task: asyncio.Task[Any] | None = None
    reader: threading.Thread | None = None
    stopping: bool = False
    notice: str | None = None
    frames: int = 0


class MediaEngine:
    """A video-only stock MTX server and at most one owned HA RTSP publisher."""

    def __init__(self, hass: Any, entry_id: str, settings: dict[str, Any], *,
                 popen: Callable[..., Any] = subprocess.Popen,
                 clock: Callable[[], float] = time.monotonic,
                 open_timeout: float = 10.0, reap_timeout: float = 2.0) -> None:
        if not isinstance(entry_id, str) or not _ENTRY.fullmatch(entry_id):
            raise EngineError("invalid_entry_id")
        if any(type(value) not in (int, float) or not math.isfinite(value) or value <= 0 for value in (open_timeout, reap_timeout)):
            raise EngineError("invalid_timeout")
        self._hass, self._entry_id = hass, entry_id
        self._settings = validate_settings(settings)
        self._popen, self._clock = popen, clock
        self._open_timeout, self._reap_timeout = open_timeout, reap_timeout
        self._publisher_password = secrets.token_hex(32)
        self._lock = threading.RLock()
        self._operations = asyncio.Lock()
        self._thread_stop = threading.Event()
        self._watcher: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._callback: Callable[[str], Any] | None = None
        self._server: Any = None
        self._server_spawn: asyncio.Task[Any] | None = None
        self._prepare_task: asyncio.Task[Any] | None = None
        self._worker: _Worker | None = None
        self._deadline = 0.0
        self._fault = False
        self._stopping = False
        self._closed = False
        self._server_notified = False
        self._runtime: Path | None = None
        self._runtime_lock_fd: int | None = None

    @property
    def frames(self) -> int:
        with self._lock:
            return self._worker.frames if self._worker else 0

    @property
    def running(self) -> bool:
        with self._lock:
            return self._server is not None and self._server.poll() is None and not self._stopping and not self._fault

    @property
    def video_running(self) -> bool:
        with self._lock:
            record = self._worker
            return bool(record and not record.stopping and record.frames > 0 and record.process is not None and record.process.poll() is None)

    @property
    def deadline(self) -> float:
        with self._lock:
            return self._deadline

    def set_callback(self, callback: Callable[[str], Any]) -> None:
        if not callable(callback):
            raise EngineError("invalid_callback")
        with self._lock:
            self._callback = callback

    def _notify(self, code: str, record: _Worker | None = None) -> None:
        if code not in _CALLBACK_CODES:
            return
        with self._lock:
            loop, callback = self._loop, self._callback
        if loop is not None and callback is not None:
            def deliver() -> None:
                with self._lock:
                    if record is not None and self._worker is not record:
                        return
                callback(code)
            try:
                loop.call_soon_threadsafe(deliver)
            except RuntimeError:
                pass

    def extend_deadline(self, deadline: float) -> None:
        if type(deadline) not in (int, float) or not math.isfinite(deadline) or (deadline != 0 and deadline <= self._clock()):
            raise EngineError("invalid_deadline")
        with self._lock:
            if deadline == 0:
                self._deadline = 0.0
                return  # Cancellation is idempotent, including failed/closed state.
            if self._stopping or self._closed or self._fault:
                raise EngineError("engine_unavailable")
            self._deadline = max(self._deadline, float(deadline))

    def _prepare_runtime(self) -> None:
        try:
            if platform.system() != "Linux" or platform.machine().lower() not in {"x86_64", "amd64"}:
                raise EngineError("unsupported_platform")
            base = Path(self._hass.config.path(".ha_onvif_exporter"))
            if not base.is_absolute() or base.parent.is_symlink():
                raise EngineError("runtime_permissions")
            _private_directory(base)
            runtime = base / self._entry_id
            _private_directory(runtime)
            import fcntl
            lock_path = runtime / "engine.lock"
            fd = os.open(lock_path, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
            try:
                info = os.fstat(fd)
                if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600:
                    raise EngineError("runtime_permissions")
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except Exception:
                os.close(fd)
                raise EngineError("runtime_locked") from None
            with self._lock:
                self._runtime_lock_fd, self._runtime = fd, runtime
                if self._stopping:
                    raise EngineError("engine_unavailable")
            archive = _read_archive(_COMPONENT / "assets")
            with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as bundle:
                binary_info = bundle.getmember("mediamtx")
                if not binary_info.isfile() or not 0 < binary_info.size <= 64 * 1024 * 1024:
                    raise EngineError("asset_invalid")
                binary_file = bundle.extractfile(binary_info)
                if binary_file is None:
                    raise EngineError("asset_invalid")
                binary = binary_file.read(64 * 1024 * 1024 + 1)
            if hashlib.sha256(binary).hexdigest() != BINARY_SHA256:
                raise EngineError("asset_invalid")
            standby = _read_pinned(_COMPONENT / "assets" / "standby.mp4", STANDBY_SHA256, 16 * 1024 * 1024)
            _private_file(runtime / "mediamtx", binary, 0o700)
            _private_file(runtime / "standby.mp4", standby, 0o600)
            config = json.dumps(media_config(self._settings, self._publisher_password, runtime / "standby.mp4"), ensure_ascii=True).encode("ascii")
            _private_file(runtime / "mediamtx.yml", config, 0o600)
        except EngineError:
            raise
        except Exception:
            raise EngineError("runtime_prepare_failed") from None

    def _spawn_server_process(self) -> None:
        if self._runtime is None:
            raise EngineError("engine_unavailable")
        process = self._popen([sys.executable, "-I", "-u", str(_COMPONENT / "process_guard.py"),
                              "server", str(os.getpid()), str(self._runtime / "mediamtx"), str(self._runtime / "mediamtx.yml")],
                             stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                             cwd=str(self._runtime), env=dict(_ENV), close_fds=True)
        with self._lock:
            self._server = process  # Retain handle before any deadline/cancel check.
            if self._stopping:
                self._kill_locked(process)

    async def start(self) -> None:
        async with self._operations:
            if self.running:
                return
            if self._fault or self._closed or self._stopping:
                raise EngineError("engine_unavailable")
            self._loop = asyncio.get_running_loop()
            try:
                self._prepare_task = asyncio.create_task(asyncio.to_thread(self._prepare_runtime))
                await asyncio.shield(self._prepare_task)
                self._watcher = threading.Thread(target=self._watchdog, name="ha-onvif-watchdog", daemon=True)
                self._watcher.start()
                self._server_spawn = asyncio.create_task(asyncio.to_thread(self._spawn_server_process))
                await asyncio.shield(self._server_spawn)
                start_limit = self._clock() + 3.0
                while self._clock() < start_limit:
                    if not self.running:
                        raise EngineError("server_failed")
                    if await asyncio.to_thread(self._listener_ready):
                        await asyncio.sleep(0.1)
                        if self.running:
                            return
                    await asyncio.sleep(0.05)
                raise EngineError("server_start_timeout")
            except asyncio.CancelledError:
                with self._lock:
                    self._stopping = True
                asyncio.create_task(self.stop())
                raise
            except EngineError:
                asyncio.create_task(self.stop())
                raise
            except Exception:
                asyncio.create_task(self.stop())
                raise EngineError("server_failed") from None

    def _listener_ready(self) -> bool:
        try:
            with socket.create_connection((self._settings["bind_ip"], self._settings["rtsp_port"]), timeout=0.2):
                return True
        except OSError:
            return False

    def _finish_ready(self, record: _Worker, code: str | None = None) -> None:
        def finish() -> None:
            if record.ready.done():
                return
            if code is None:
                record.ready.set_result(None)
            else:
                record.ready.set_exception(EngineError(code))
        if self._loop is not None:
            try:
                self._loop.call_soon_threadsafe(finish)
            except RuntimeError:
                pass

    def _kill_locked(self, process: Any) -> bool:
        try:
            if process.poll() is None:
                process.kill()
            return True
        except ProcessLookupError:
            return True  # Reap still has to confirm termination.
        except Exception:
            self._fault = True
            return False

    def _stop_record_locked(self, record: _Worker, code: str) -> None:
        record.stopping = True
        if record.process is not None:
            self._kill_locked(record.process)
        self._finish_ready(record, code)

    def _spawn_worker_process(self, record: _Worker, source: str) -> None:
        try:
            process = self._popen([sys.executable, "-I", "-u", str(_COMPONENT / "process_guard.py"),
                                  "worker", str(os.getpid()), str(_COMPONENT / "media_worker.py")],
                                 stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                 bufsize=0, cwd=str(self._runtime), env=dict(_ENV), close_fds=True)
            with self._lock:
                record.process = process
                if record.stopping or self._stopping or self._deadline <= self._clock():
                    self._stop_record_locked(record, "expired")
                    return
            config = {"source": source, "sink_host": self._settings["bind_ip"], "rtsp_port": self._settings["rtsp_port"],
                      "publisher_user": "window_publisher", "publisher_password": self._publisher_password}
            payload = json.dumps(config, ensure_ascii=True).encode("ascii") + b"\n"
            if len(payload) > 4096 or process.stdin.write(payload) != len(payload):
                raise EngineError("worker_failed")
            process.stdin.close()
            record.reader = threading.Thread(target=self._read_progress, args=(record,), name="ha-onvif-progress", daemon=True)
            record.reader.start()
        except Exception:
            with self._lock:
                self._stop_record_locked(record, "worker_failed")
            self._notify_record(record, "worker_failed")
        finally:
            record.spawn_done.set()

    def _notify_record(self, record: _Worker, code: str) -> None:
        with self._lock:
            if record.notice is not None:
                return
            record.notice = code
        self._notify(code, record)

    def _read_progress(self, record: _Worker) -> None:
        try:
            while True:
                raw = record.process.stdout.readline(257)
                if not raw:
                    with self._lock:
                        if record.stopping:
                            return
                    raise EngineError("worker_failed")
                if len(raw) > 256 or not raw.endswith(b"\n"):
                    raise EngineError("worker_failed")
                def unique(items):
                    result = {}
                    for key, value in items:
                        if key in result:
                            raise ValueError
                        result[key] = value
                    return result
                data = json.loads(raw.decode("utf-8", errors="strict"), object_pairs_hook=unique)
                frames = data.get("frames") if isinstance(data, dict) else None
                if not isinstance(data, dict) or set(data) != {"frames"} or type(frames) is not int or not 0 < frames < 2**53:
                    raise EngineError("worker_failed")
                with self._lock:
                    if record is not self._worker or record.stopping or self._stopping:
                        return
                    if self._deadline <= self._clock():
                        self._stop_record_locked(record, "expired")
                        notice = "expired"
                    elif frames <= record.frames:
                        raise EngineError("worker_failed")
                    else:
                        record.frames = frames
                        self._finish_ready(record)
                        notice = None
                if notice:
                    self._notify_record(record, notice)
                    return
        except Exception:
            with self._lock:
                if record.stopping:
                    return
                self._stop_record_locked(record, "worker_failed")
            self._notify_record(record, "worker_failed")

    async def start_video(self, source: str) -> None:
        if not isinstance(source, str) or not _SOURCE.fullmatch(source):
            raise EngineError("invalid_source")
        async with self._operations:
            if not self.running or self._worker is not None or self._deadline <= self._clock():
                raise EngineError("engine_unavailable")
            record = _Worker(asyncio.get_running_loop().create_future())
            record.ready.add_done_callback(lambda future: None if future.cancelled() else future.exception())
            with self._lock:
                self._worker = record
            record.spawn_task = asyncio.create_task(asyncio.to_thread(self._spawn_worker_process, record, source))
        try:
            async with asyncio.timeout(self._open_timeout):
                await asyncio.shield(record.spawn_task)
                await asyncio.shield(record.ready)
            with self._lock:
                if record.stopping or self._deadline <= self._clock() or not self.running:
                    raise EngineError("engine_unavailable")
        except asyncio.CancelledError:
            await self.stop_video()
            raise
        except (TimeoutError, EngineError):
            await self.stop_video()
            raise EngineError("worker_failed") from None

    def _watchdog_once(self) -> None:
        with self._lock:
            if self._stopping:
                return
            if self._server is not None and self._server.poll() is not None:
                self._fault = True
                if self._worker is not None:
                    self._stop_record_locked(self._worker, "server_failed")
                if not self._server_notified:
                    self._server_notified = True
                    self._notify("server_failed")
                return
            record = self._worker
            if record is None or record.stopping:
                return
            if self._deadline <= self._clock():
                self._stop_record_locked(record, "expired")
                self._notify_record(record, "expired")
            elif record.process is not None and record.process.poll() is not None:
                self._stop_record_locked(record, "worker_failed")
                self._notify_record(record, "worker_failed")

    def _watchdog(self) -> None:
        while not self._thread_stop.wait(0.05):
            self._watchdog_once()

    def _reap_worker(self, record: _Worker) -> bool:
        if not record.spawn_done.wait(self._reap_timeout):
            return False
        with self._lock:
            process = record.process
            if process is None:
                return True  # Popen failed before a child was created.
            if not self._kill_locked(process):
                return False
        try:
            process.wait(timeout=self._reap_timeout)
            if process.poll() is None:
                return False
        except Exception:
            return False
        if record.reader is not None:
            record.reader.join(timeout=self._reap_timeout)
            if record.reader.is_alive():
                return False
        for stream in (process.stdin, process.stdout):
            try:
                if stream is not None:
                    stream.close()
            except Exception:
                return False
        return True

    async def _stop_video(self) -> bool:
        with self._lock:
            record = self._worker
            self._deadline = 0.0
            if record is None:
                return True
            self._stop_record_locked(record, "worker_stopped")
        confirmed = await asyncio.to_thread(self._reap_worker, record)
        with self._lock:
            if confirmed and self._worker is record:
                self._worker = None
            elif not confirmed:
                self._fault = True
        return confirmed

    async def stop_video(self) -> bool:
        async with self._operations:
            return await self._stop_video()

    def _reap_server(self) -> bool:
        with self._lock:
            process = self._server
            if process is None:
                return True
            if not self._kill_locked(process):
                return False
        try:
            process.wait(timeout=self._reap_timeout)
            if process.poll() is None:
                return False
        except Exception:
            return False
        with self._lock:
            self._server = None
        return True

    async def stop(self) -> bool:
        async with self._operations:
            with self._lock:
                self._stopping = True
                self._deadline = 0.0
            self._thread_stop.set()
            worker_confirmed = await self._stop_video()
            tasks_confirmed = True
            for task in (self._prepare_task, self._server_spawn):
                if task is not None:
                    try:
                        async with asyncio.timeout(self._reap_timeout):
                            await asyncio.shield(task)
                    except TimeoutError:
                        tasks_confirmed = False
                    except Exception:
                        pass
            server_confirmed = await asyncio.to_thread(self._reap_server)
            if self._watcher is not None:
                await asyncio.to_thread(self._watcher.join, self._reap_timeout)
                tasks_confirmed = tasks_confirmed and not self._watcher.is_alive()
            confirmed = worker_confirmed and server_confirmed and tasks_confirmed
            with self._lock:
                if confirmed:
                    if self._runtime_lock_fd is not None:
                        os.close(self._runtime_lock_fd)
                        self._runtime_lock_fd = None
                    self._closed = True
                else:
                    self._fault = True
            return confirmed
