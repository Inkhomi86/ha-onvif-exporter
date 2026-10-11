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
from .engine import WORKER_ERROR_CODES


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
        self._activation = 0
        self._publisher_activation = 0
        self._publisher_generation = 0
        self._finishers = 0
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

    def _media_failure(self, fallback):
        code = getattr(self.engine, "failure_code", None)
        return code if type(code) is str and code in WORKER_ERROR_CODES | {"worker_failed", "server_failed"} else fallback

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
        deadline = max(self.deadline, activated + self.window_seconds)
        try:
            self.engine.extend_deadline(deadline)
        except Exception:
            self.status, self.error_code = "fault", self._media_failure("activation_failed")
            self._notify()
            return False
        self._activation += 1  # Only an authorized activation permits another attempt.
        if self.motion:
            self._publisher_activation = self._activation
        self.deadline = deadline
        self._arm_expiry()
        self._queue_open()
        self._notify()
        return True

    def _queue_open(self):
        if (self._started and not self._closing and self.enabled and not self._poisoned
                and not self.motion and not self._finishers and self.deadline > time.monotonic()
                and (self._start_task is None or self._start_task.done())):
            self._start_task = self._task(self._open())
            self._start_task.add_done_callback(self._opening_done)

    def _opening_done(self, task):
        if self._start_task is task:
            self._start_task = None
            self._queue_open()  # Failure clears its own window; only a renewal survives.

    async def _cancel_open(self):
        task = self._start_task
        if task is not None and task is not asyncio.current_task() and not task.done():
            if not task.cancelling():
                task.cancel()
            await asyncio.shield(asyncio.gather(task, return_exceptions=True))

    async def _complete_cleanup(self, coroutine):
        """Repeated caller cancellation cannot interrupt our owned cleanup."""
        task = asyncio.create_task(coroutine)
        cancelled = False
        while True:
            try:
                result = await asyncio.shield(task)
                break
            except asyncio.CancelledError:
                if task.done():
                    raise
                cancelled = True
        if cancelled:
            raise asyncio.CancelledError
        return result

    async def _join_open_children(self, children):
        for task in children:
            if not task.done() and not task.cancelling():
                task.cancel()
        if children:
            await asyncio.gather(*children, return_exceptions=True)

    async def _cleanup_open(self, children):
        # In managed begin mode canceling waiters never closes WebRTC. Join the
        # publisher task first, then require explicit confirmed reap before RTC.
        await self._join_open_children(children)
        return await self._close_owned()

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
            publisher_attempted = False
            attempt = self._activation
            children = []
            live = cancelled = failed = False
            try:
                info = await self.consumer.begin(self.settings[CONF_CAMERA_ENTITY])
                if self.deadline <= time.monotonic() or self._closing or not self.enabled:
                    return
                if info.source is None:
                    raise RuntimeError("managed_source_required")
                self.engine.extend_deadline(self.deadline)
                publisher_attempted = True
                self._publisher_activation = attempt
                self._publisher_generation += 1
                publisher = asyncio.create_task(self.engine.start_video(info.source))
                frame = asyncio.create_task(self.consumer.wait_frame())
                failure = asyncio.create_task(self.consumer.wait_failure())
                children = [publisher, frame, failure]
                pending = set(children)
                while not (publisher.done() and frame.done()):
                    done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                    if failure in done or any(task.cancelled() or task.exception() is not None
                                              for task in done if task is not failure):
                        raise RuntimeError("opening_failed")
                # Consume both results without exposing exceptions or TaskGroups.
                if (publisher.cancelled() or frame.cancelled() or publisher.exception() is not None
                        or frame.exception() is not None or failure.done()):
                    raise RuntimeError("opening_failed")
                await self._join_open_children([failure])
                if self.deadline <= time.monotonic() or self._closing or not self.enabled:
                    return
                if (self.consumer.session_id != info.session_id or not self.consumer.active
                        or not self.consumer.frame_ready or not self.engine.video_running):
                    raise RuntimeError("opening_failed")
                self.motion, self.status = True, "live"
                self._publisher_activation = self._activation
                self.onvif.set_motion(True)
                self._notify()
                self._watch_task = self._task(self._watch_consumer(self.consumer.session_id))
                live = True
            except asyncio.CancelledError:
                cancelled = True
            except Exception:
                failed = True
                if self._activation == attempt:
                    self.deadline = 0
                    self.engine.extend_deadline(0)
                    if self._timer:
                        self._timer.cancel()
                        self._timer = None
            finally:
                if not live:
                    await self._complete_cleanup(self._cleanup_open(children))
            if failed:
                if self._activation == attempt or self._poisoned:
                    if publisher_attempted and not self._poisoned:
                        self.error_code = self._media_failure("activation_failed")
                    elif not self._poisoned:
                        self.error_code = "activation_failed"
                    self.status = "fault"
                self._notify()
            if cancelled:
                raise asyncio.CancelledError

    async def _watch_consumer(self, session_id):
        await self.consumer.wait_failure()
        if self.consumer.session_id == session_id and self.motion and not self._closing:
            if self._watch_task is asyncio.current_task():
                self._watch_task = None  # A cleanup child must not join its own parent.
            await self._finish("consumer_failed", clear=True, expected_session=session_id,
                               expected_activation=self._activation)

    def _engine_changed(self, code):
        if self._started and not self._closing and code in {"expired", "worker_failed", "server_failed"}:
            self._task(self._finish(code, clear=code != "expired",
                                   expected_session=self.consumer.session_id,
                                   expected_activation=self._publisher_activation,
                                   expected_publisher=self._publisher_generation))

    async def _close_owned(self):
        self.status = "stopping"
        self._notify()
        if self._watch_task is not None and self._watch_task is not asyncio.current_task():
            self._watch_task.cancel()
            await asyncio.gather(self._watch_task, return_exceptions=True)
        self._watch_task = None
        # Reap libav's RTSP reader before dropping the HA WebRTC reference.
        try:
            confirmed = await self.engine.stop_video()
        except Exception:
            confirmed = False
        if confirmed is not True:
            self._poisoned, self.error_code, self.status = True, "publisher_cleanup_unconfirmed", "fault"
            self.motion = False
            self.onvif.set_motion(False)
            self._notify()
            return False
        self.motion = False
        self.onvif.set_motion(False)
        self._notify()  # The confirmed publisher stop already exposes STANDBY.
        had_session = self.consumer.session_id is not None
        try:
            await self.consumer.close()
        except Exception:
            self._poisoned, self.error_code = True, "consumer_cleanup_unconfirmed"
        if had_session and not self.consumer.cleanup_ok:
            self._poisoned, self.error_code = True, "consumer_cleanup_unconfirmed"
        self.status = "fault" if self._poisoned else ("standby" if self.enabled else "disabled")
        self._notify()
        return not self._poisoned

    async def _finish(self, reason, *, clear, expiry_deadline=None,
                      expected_session=None, expected_activation=None, expected_publisher=None):
        if expected_publisher is not None and expected_publisher != self._publisher_generation:
            return
        if expected_session is not None and self.consumer.session_id != expected_session:
            return  # A queued notification cannot close a later HA session.
        if expiry_deadline is not None and self.deadline > time.monotonic():
            return  # No physical worker kill happened for this stale HA timer.
        renewed = expected_activation is not None and expected_activation != self._activation
        attempt = self._activation
        if clear and (not renewed or reason == "server_failed"):
            self.deadline = 0
            self.engine.extend_deadline(0)
            if self._timer:
                self._timer.cancel()
                self._timer = None
        self._finishers += 1
        try:
            await self._cancel_open()  # Outside the lock owned by the opening task.
            async with self._lock:
                if expected_session is not None and self.consumer.session_id not in (expected_session, None):
                    return
                if expiry_deadline is not None and self.deadline > time.monotonic():
                    return  # Opening was canceled before a genuine renewal arrived.
                if (reason == "expired" and expiry_deadline is None and self.deadline > time.monotonic()
                        and self.motion and self.consumer.active and self.consumer.frame_ready):
                    # The watchdog has killed the old publisher. A fresh
                    # activation needs a replacement, not an ignored callback.
                    # Keep the healthy scoped HA reference while replacing it.
                    self.motion = False
                    self.onvif.set_motion(False)
                    try:
                        confirmed = await self.engine.stop_video()
                    except Exception:
                        confirmed = False
                    if confirmed is not True:
                        await self._complete_cleanup(self._close_owned())
                        return
                    try:
                        self.engine.extend_deadline(self.deadline)
                        self._publisher_generation += 1
                        self._publisher_activation = attempt
                        await self.engine.start_video(self.consumer.source)
                        if (self.deadline <= time.monotonic() or not self.consumer.active
                                or not self.engine.video_running or self._closing or not self.enabled):
                            raise RuntimeError("opening_failed")
                        self.motion, self.status = True, "live"
                        self._publisher_activation = self._activation
                        self.onvif.set_motion(True)
                    except BaseException:
                        if self._activation == attempt:
                            self.deadline = 0
                            self.engine.extend_deadline(0)
                        await self._complete_cleanup(self._close_owned())
                        if self._activation == attempt or self._poisoned:
                            self.status = "fault"
                            if not self._poisoned:
                                self.error_code = self._media_failure("activation_failed")
                    self._notify()
                    return
                await self._complete_cleanup(self._close_owned())
                if (reason not in {"expired", "manual", "disabled", "shutdown"} and not renewed
                        and (self._activation == attempt or reason == "server_failed" or self._poisoned)):
                    self.status = "fault"
                    if not self._poisoned:
                        self.error_code = self._media_failure(reason) if reason in {"worker_failed", "server_failed"} else reason
                    self._notify()
        finally:
            self._finishers -= 1
            self._queue_open()

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
        await self._cancel_open()
        await self._finish("shutdown", clear=True)
        for task in tuple(self._tasks):
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.onvif.stop()
        confirmed = await self.engine.stop()
        return confirmed and not self._poisoned
