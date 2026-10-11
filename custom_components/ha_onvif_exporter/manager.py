"""Own one HA consumer and publisher, never the underlying door/camera."""
from __future__ import annotations

import asyncio
import time

from .const import (
    CONF_BINARY_SENSOR, CONF_CAMERA_ENTITY, CONF_EVENT_AT_FIELD, CONF_EVENT_FILTER,
    CONF_EVENT_TYPE, CONF_TRIGGER_MODE, CONF_WINDOW_SECONDS,
    TRIGGER_BINARY_SENSOR, TRIGGER_EVENTS,
)
from .policy import EventAdmission


class ExportManager:
    """Last-activation window; frames and ONVIF queries cannot extend it."""

    def __init__(self, hass, entry_id, settings, consumer, engine, onvif):
        self.hass, self.entry_id, self.settings = hass, entry_id, settings
        self.consumer, self.engine, self.onvif = consumer, engine, onvif
        self.status, self.error_code = "standby", None
        self.enabled, self.motion = True, False
        self.deadline = 0.0
        self.window_seconds = settings[CONF_WINDOW_SECONDS]
        self._lock = asyncio.Lock()
        self._closing = False
        self._poisoned = False
        self._listeners = set()
        self._unsubscribers = []
        self._tasks = set()
        self._start_task = self._watch_task = self._timer = None
        self._started = False
        self._events = EventAdmission(settings[CONF_EVENT_FILTER], settings[CONF_EVENT_AT_FIELD])
        engine.set_callback(self._engine_changed)

    @property
    def remaining(self):
        return max(0, round(self.deadline - time.monotonic(), 1))

    def listen(self, callback):
        self._listeners.add(callback)
        return lambda: self._listeners.discard(callback)

    def _notify(self):
        for callback in tuple(self._listeners):
            try:
                callback()
            except Exception:
                # A detached/broken entity must never interrupt media cleanup.
                pass

    def _task(self, coro):
        task = self.hass.async_create_task(coro, eager_start=False)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        # Retrieve unexpected exceptions without ever logging URLs/credentials.
        task.add_done_callback(lambda t: None if t.cancelled() else t.exception())
        return task

    async def start(self):
        await self.engine.start()
        try:
            await self.onvif.start()
        except BaseException:
            await self.engine.stop()
            raise
        self._started = True
        mode = self.settings[CONF_TRIGGER_MODE]
        if mode == TRIGGER_EVENTS:
            def event_received(event):
                if self._events.accept(event.data):
                    self._task(self.async_activate(activation_at=time.monotonic()-self._events.last_age))
            self._unsubscribers.append(self.hass.bus.async_listen(self.settings[CONF_EVENT_TYPE], event_received))
        elif mode == TRIGGER_BINARY_SENSOR:
            from homeassistant.helpers.event import async_track_state_change_event
            def sensor_changed(event):
                old, new = event.data.get("old_state"), event.data.get("new_state")
                # No activation from startup, restored on, unknown or unavailable.
                if old is not None and new is not None and old.state == "off" and new.state == "on":
                    self._task(self.async_activate())
            self._unsubscribers.append(async_track_state_change_event(self.hass, [self.settings[CONF_BINARY_SENSOR]], sensor_changed))

    async def async_activate(self, *, activation_at=None):
        if not self._started or self._closing or not self.enabled or self._poisoned:
            return False
        self.error_code = None
        now = time.monotonic()
        activated = now if activation_at is None else activation_at
        remaining = activated + self.window_seconds - now
        if remaining <= 0:
            return False
        self.deadline = max(self.deadline, activated + self.window_seconds)
        self.engine.extend_deadline(self.deadline)
        self._arm_expiry()
        if self._start_task is None or self._start_task.done():
            self._start_task = self._task(self._open())
        self._notify()
        return True

    def _arm_expiry(self):
        if self._timer is not None:
            self._timer.cancel()
        self._timer = asyncio.get_running_loop().call_later(max(0, self.deadline - time.monotonic()), self._expired)

    def _expired(self):
        self._timer = None
        if self.deadline > time.monotonic():
            self._arm_expiry()
        else:
            self._task(self._finish("expired", clear=False, expiry_deadline=self.deadline))

    async def _open(self):
        async with self._lock:
            if self.motion or self._closing or not self.enabled or self.deadline <= time.monotonic() or self._poisoned:
                return
            self.status = "starting"
            self._notify()
            try:
                info = await self.consumer.open(self.settings[CONF_CAMERA_ENTITY])
                if self.deadline <= time.monotonic() or self._closing or not self.enabled:
                    await self._close_owned()
                    return
                if info.source is None:
                    raise RuntimeError("managed_source_required")
                self.engine.extend_deadline(self.deadline)
                await self.engine.start_video(info.source)
                if self.deadline <= time.monotonic() or self._closing or not self.enabled:
                    await self._close_owned()
                    return
                self.motion, self.status = True, "live"
                self.onvif.set_motion(True)
                self._notify()
                self._watch_task = self._task(self._watch_consumer(self.consumer.session_id))
            except asyncio.CancelledError:
                await self._close_owned()
                raise
            except Exception:
                self.error_code = "activation_failed"
                self.deadline = 0
                await self._close_owned()
                self.status = "fault"
                self._notify()

    async def _watch_consumer(self, session_id):
        await self.consumer.wait_closed()
        if self.consumer.session_id == session_id and self.motion and not self._closing:
            await self._finish("consumer_failed", clear=True)

    def _engine_changed(self, code):
        if self._started and not self._closing and code in {"expired", "worker_failed", "server_failed"}:
            self._task(self._finish(code, clear=code != "expired"))

    async def _close_owned(self):
        self.status = "stopping"
        self._notify()
        if self._watch_task is not None and self._watch_task is not asyncio.current_task():
            self._watch_task.cancel()
            await asyncio.gather(self._watch_task, return_exceptions=True)
        self._watch_task = None
        # Reap libav's RTSP reader before dropping the HA WebRTC reference.
        confirmed = await self.engine.stop_video()
        if not confirmed:
            self._poisoned, self.error_code, self.status = True, "publisher_cleanup_unconfirmed", "fault"
            self.motion = False
            self.onvif.set_motion(False)
            self._notify()
            return False
        had_session = self.consumer.session_id is not None
        await self.consumer.close()
        if had_session and not self.consumer.cleanup_ok:
            self._poisoned, self.error_code = True, "consumer_cleanup_unconfirmed"
        self.motion = False
        self.onvif.set_motion(False)
        self.status = "fault" if self._poisoned else ("standby" if self.enabled else "disabled")
        self._notify()
        return not self._poisoned

    async def _finish(self, reason, *, clear, expiry_deadline=None):
        if clear:
            self.deadline = 0
            self.engine.extend_deadline(0)
            if self._timer:
                self._timer.cancel()
                self._timer = None
        async with self._lock:
            if expiry_deadline is not None and self.deadline > time.monotonic():
                # A queued timer from the old window cannot close its renewal.
                return
            if reason == "expired" and expiry_deadline is None and self.deadline > time.monotonic() and self.consumer.frame_ready:
                # The watchdog has already killed the old publisher. A fresh
                # activation needs a new publisher, not an ignored callback.
                # Keep the healthy scoped HA reference while replacing it.
                if not await self.engine.stop_video():
                    await self._close_owned()
                    return
                try:
                    self.engine.extend_deadline(self.deadline)
                    await self.engine.start_video(self.consumer.source)
                except Exception:
                    self.deadline = 0
                    await self._close_owned()
                    self.status, self.error_code = "fault", "activation_failed"
                self._notify()
                return
            await self._close_owned()
            if reason not in {"expired", "manual", "disabled", "shutdown"}:
                self.status, self.error_code = "fault", reason
                self._notify()
        if not clear and self.deadline > time.monotonic() and not self._poisoned and not self._closing:
            self.engine.extend_deadline(self.deadline)
            self._start_task = self._task(self._open())

    async def async_deactivate(self):
        await self._finish("manual", clear=True)

    async def async_set_enabled(self, enabled):
        self.enabled = bool(enabled)
        if not self.enabled:
            await self._finish("disabled", clear=True)
        elif not self._poisoned:
            self.status = "standby"
            self._notify()

    async def stop(self):
        self._closing, self._started = True, False
        for unsubscribe in self._unsubscribers:
            unsubscribe()
        self._unsubscribers.clear()
        self.deadline = 0
        self.engine.extend_deadline(0)
        if self._timer:
            self._timer.cancel()
            self._timer = None
        if self._start_task and not self._start_task.done():
            self._start_task.cancel()
            await asyncio.gather(self._start_task, return_exceptions=True)
        await self._finish("shutdown", clear=True)
        for task in tuple(self._tasks):
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.onvif.stop()
        confirmed = await self.engine.stop()
        return confirmed and not self._poisoned
