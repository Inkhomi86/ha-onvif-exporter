# Owned HA camera consumer

`HACameraConsumer(hass, managed_go2rtc=True)` provides `await open(entity_id)`
and `await close()`. Open returns `ConsumerInfo(entity_id, session_id, source)`
only after receiving a real video frame. `source` is the alias returned by HA's
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
window. Watch `wait_closed()` and `failure_code`; fall back to STANDBY on failure.
Do not use `bticino_c300x.stop_doorbell_video` to stop an export window.

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
