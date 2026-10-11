# Private video output worker

The worker reads HA's shared go2rtc alias, never a native camera URL. It does
not modify the source camera's integration, firmware, or runtime configuration.

One child/window; stdin is exactly one UTF-8 JSON line <=8192 bytes with exactly
`{source, sink_host, rtsp_port, publisher_user, publisher_password}`. Duplicate
keys/extras are rejected. Source grammar matches consumer's encoded alias and
fixed `rtsp://127.0.0.1:18554/` prefix. Sink is strict IPv4 plus integer port >=1024;
the parent must pin its configured local bind IP and port. Internal user is
fixed `window_publisher` and password is generated `[a-z0-9]{64}`. Publish path is
fixed `/standby`. URLs and secrets remain in child memory and stdin, not argv.

Launch with an exact owned subprocess handle, stderr=DEVNULL, private stdin,
and a bounded stdout parser. Worker stdout is only `{"frames": N}` after a
successful packet mux. This is the readiness barrier, NOT Popen/HA SDP answer.
Worker errors are controlled codes only. Do not emit libav exception strings.

Pipeline: video-only TCP RTSP decode -> scale/pad preserving aspect ratio ->
square pixels -> fps30 -> yuv420p -> libx264 Main1920x1080/30, GOP10, no B-frames,
zerolatency, one thread -> authenticated MediaMTX TCP RTSP. Initial exporter is
video-only; HA audio, microphone, and talkback remain managed by the camera.

The parent holds a real HA WebRTC reference and receives a frame BEFORE this
worker opens the alias. It owns the last-activation+30s deadline, external
watchdog, termination/reap, and fallback STANDBY. Kill/reap this reader BEFORE
closing that WebRTC reference. SIGTERM is a cooperative stop flag, not a hard
guarantee against blocked libav I/O; use a bounded grace followed by exact-child
kill/reap. Libav open/read/mux operations have1s network timeouts. There are no
native API calls, unlocks, callback registration, retries, or renewals here.

Existing-source PTS are preserved relative to the first valid timestamp. Up to
30 total missing-PTS frames are skipped, as the earlier synthetic RTSP fixture
required; the fps filter fills valid timestamp gaps. The worker fails closed on
timestamp regression or time-base changes rather than inventing timestamps.
This is policy, not diagnosis of any real camera.

Accepted PyAV versions:17.0.1 (installed HA runtime) and19.0.1 (existing local
synthetic fixture). Source APIs were checked against official17.0.1 graph,
container, and logging implementations. Runtime requires libx264 and stock
buffer/scale/pad/setsar/fps/format/buffersink filters. No PyAV upgrade/downgrade is
requested. Local unit tests isolate the protocol/pipeline. Actual HA17 media,
Protect/AI adoption, shared RTSP/source changes, and Door Entry coexistence still
need staged real validation; these tests do not certify those behaviors.

- [PyAV17.0.1 filter graph](https://github.com/PyAV-Org/PyAV/blob/v17.0.1/av/filter/graph.py)
- [PyAV17.0.1 container](https://github.com/PyAV-Org/PyAV/blob/v17.0.1/av/container/core.py)
- [PyAV17.0.1 logging](https://github.com/PyAV-Org/PyAV/blob/v17.0.1/av/logging.py)
