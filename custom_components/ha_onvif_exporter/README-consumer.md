# Owned HA camera consumer

`HACameraConsumer(hass, managed_go2rtc=True)` provides two readiness stages:

- `await begin(entity_id)` returns `OpenedCamera(entity_id, session_id, source)`
  after the SDP answer has been accepted by the local WebRTC peer. This is an
  owned, receive-only HA session; it does **not** assert receipt of a frame.
- `await wait_frame()` returns the same metadata only after an actual video
  frame and its optional callback have succeeded. Both stages share one absolute
  timeout, 15 seconds by default, starting when the session is reserved. A call
  to `wait_frame` never starts another 15-second allowance.
- `await close()` explicitly releases that exact owned session.

The legacy `await open(entity_id)` waits for both stages and retains automatic
cleanup on failure/cancellation. `ConsumerInfo` remains an alias of
`OpenedCamera` for compatibility. `source` is the alias returned by HA's
`get_camera_identifier`, fixed to `rtsp://127.0.0.1:18554/<alias>`, never the
underlying camera URL. No HA bearer token or native-agent credential is needed.

The generic class defaults to an aiortc receive-only video + audio offer.
The exporter manager explicitly passes `receive_audio=False`: this release
receives and publishes video only, without requesting the camera's audio.
The backend calls HA's camera API, handles answers and candidates, and drains
the selected tracks.
It does not answer a ring, send microphone audio, invoke an unlock, call
`stream_source`, or access any camera-specific API/session dictionaries.
`on_frame(frame)` optionally consumes received video directly (fast callback or
bounded awaitable). Generic callers that opt into audio only drain it; the
exporter's RTSP worker does not publish audio. Still-image-only cameras are rejected.
Camera-specific WebRTC data-channel protocols are not supported in this initial
go2rtc-oriented release; cameras requesting one are rejected before an offer.

The external manager owns activation deduplication and the 30 s deadline from
the latest valid activation. It must kill and await its RTSP output worker
**before** calling consumer.close(). Keep the consumer open for the whole export
window. In managed `begin` mode, watch `wait_failure()` and `failure_code`; fall
back to STANDBY on failure. `wait_closed()` still means cleanup has completed,
not merely that a failure was detected.
Do not use `bticino_c300x.stop_doorbell_video` to stop an export window.

Once `begin` has returned, failure, startup timeout, or cancellation of
`wait_frame` signals failure without closing either the peer or HA's scoped
camera reference. `active` and `frame_ready` become false, but `session_id` and
`source` remain available. The caller must cancel/join its setup tasks, kill and
reap its RTSP reader, and only then call `close`. An unclosed failed managed
session rejects another `begin` with `consumer_busy`; no generation can bypass
this fence. This also applies to failures after a frame was previously received.

The exporter can start its local-alias publisher concurrently with `wait_frame`
after `begin`, but may assert motion/live only after **both** the WebRTC frame
and the publisher's first successfully muxed video packet are confirmed. SDP,
track registration, a process spawn, or a previous generation's frame is not
either readiness barrier. `begin` errors before handing off the session still
clean up automatically because no reader may have been started yet.

`managed_go2rtc=True` is an explicit deployment prerequisite. A custom camera
may implement native WebRTC without using go2rtc; this generic class cannot infer
that a shared RTSP alias exists. Only use alias mode after verifying the selected
camera is served by HA's managed go2rtc instance. `on_frame` without alias mode
does not need that prerequisite. Receiving a WebRTC frame is not proof that the
separate RTSP publisher has already muxed/recorded a frame.

Cleanup uses the public `camera.close_webrtc_session(own_session_id)` callback,
as HA 2026.8.1 itself does when a client unsubscribes. The callback is synchronous
and may schedule asynchronous integration cleanup: close does not prove physical
BUS idleness. Stock camera's concurrency, ring preview transitions, UI Stop, and
Door Entry priority remain its own policy. This module does not add atomic native
ownership guarantees. Those combinations need real acceptance tests before
claiming preservation of all camera/talkback/phone behavior.

`cleanup_ok` describes local peer/tasks and the scoped callback only. If any
cleanup stage fails or times out, it is false and this instance rejects reopening
with `consumer_cleanup_unconfirmed`. The manager must fail closed/disable export
rather than constructing another consumer to bypass that fence. It must not
treat a completed cleanup event as proof that an unclosed transport disappeared.

Pinned public contracts:

- [HA 2026.8.1 Camera API](https://github.com/home-assistant/core/blob/2026.8.1/homeassistant/components/camera/__init__.py)
- [HA 2026.8.1 WebRTC subscription close](https://github.com/home-assistant/core/blob/2026.8.1/homeassistant/components/camera/webrtc.py)
- [HA go2rtc shared alias](https://github.com/home-assistant/core/blob/2026.8.1/homeassistant/components/go2rtc/util.py)
- [aiortc 1.15.0](https://github.com/aiortc/aiortc/tree/1.15.0)

Runtime requirements for the integration manifest: `aiortc==1.15.0`. Real tests
must check installation compatibility with HA's existing PyAV version. Unit
tests inject a peer and camera boundary; no production/network/device access.
