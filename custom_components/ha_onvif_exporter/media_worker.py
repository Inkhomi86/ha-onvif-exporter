#!/usr/bin/env python3
"""One owned HA/go2rtc reader -> normalized video-only MediaMTX publisher.

Run ONLY in a private child process, with stderr=DEVNULL. Its parent owns the
30-second window and must terminate/reap this worker BEFORE closing the owned
HA WebRTC reference. URLs/credentials come from a pipe, never command-line args.
No activation, camera/native API, door action, retries, or HA credentials.
"""
from __future__ import annotations

from fractions import Fraction
import ipaddress
import json
import logging
import re
import signal
import sys
import threading
import urllib.parse

VERSIONS = frozenset({"17.0.1", "19.0.1"})
SIZE = (1920, 1080)
RATE = 30
MAX_CONFIG = 8192
# Closed diagnostic protocol. Keep synchronized with engine.WORKER_ERROR_CODES;
# never stringify an arbitrary exception or send libav's retained details.
WORKER_ERROR_CODES = frozenset({
    "arguments", "pyav_version", "encoder_missing", "filters_missing", "url_shape",
    "config_bound", "config_schema", "config_source", "config_sink", "config_user",
    "config_password", "config_read", "video_track_count", "input_timestamp_missing",
    "input_timestamp_invalid", "input_timestamp_changed", "input_timestamp_regressed",
    "media_pipeline_source_open", "media_pipeline_output_open",
    "media_pipeline_source_demux", "media_pipeline_input_decode",
    "media_pipeline_filter_create", "media_pipeline_filter_push", "media_pipeline_filter_pull",
    "media_pipeline_output_encoder_config", "media_pipeline_output_encode",
    "media_pipeline_output_mux", "media_pipeline_output_flush", "media_pipeline_progress_emit",
    "worker_internal",
})
_SOURCE = re.compile(r"rtsp://127\.0\.0\.1:18554/(?:[A-Za-z0-9._-]|%[0-9A-Fa-f]{2}){1,1024}\Z")
_PASSWORD = re.compile(r"[a-z0-9]{64}\Z")


class WorkerError(Exception):
    """Fixed non-sensitive code; never wrap libav/URL/credential exceptions."""


def silent_av():
    import av

    if av.__version__ not in VERSIONS:
        raise WorkerError("pyav_version")
    # This is a dedicated process, NOT HA's interpreter. None disables libav's
    # logging callback completely, including internal retained URL/error strings.
    av.logging.set_level(None)
    logger = logging.getLogger("libav")
    logger.handlers[:] = [logging.NullHandler()]
    logger.propagate = False
    if "libx264" not in av.codecs_available:
        raise WorkerError("encoder_missing")
    required = {"buffer", "scale", "pad", "setsar", "fps", "format", "buffersink"}
    if not required.issubset(av.filter.filters_available):
        raise WorkerError("filters_missing")
    return av


def validate_url(value):
    """Basic libav input boundary; CLI policy is stricter in stdin_config."""
    if not isinstance(value, str) or not 1 <= len(value) <= 2048:
        raise WorkerError("url_shape")
    if any(ord(char) <= 32 or ord(char) == 127 for char in value):
        raise WorkerError("url_shape")
    try:
        parsed = urllib.parse.urlsplit(value)
        port = parsed.port
    except ValueError:
        raise WorkerError("url_shape") from None
    if (parsed.scheme != "rtsp" or not parsed.hostname or not port
            or not parsed.path.startswith("/") or parsed.query or parsed.fragment):
        raise WorkerError("url_shape")
    return value


def stdin_config(stream):
    """Accept exactly one bounded UTF-8 JSON line from our owning HA process."""
    try:
        raw = stream.readline(MAX_CONFIG + 1)
        if not raw.endswith(b"\n") or len(raw) > MAX_CONFIG:
            raise WorkerError("config_bound")

        def pairs(items):
            result = {}
            for key, value in items:
                if key in result:
                    raise WorkerError("config_schema")
                result[key] = value
            return result

        config = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs)
        required = {"source", "sink_host", "rtsp_port", "publisher_user", "publisher_password"}
        if not isinstance(config, dict) or set(config) != required:
            raise WorkerError("config_schema")
        source = config["source"]
        if not isinstance(source, str) or not _SOURCE.fullmatch(source):
            raise WorkerError("config_source")
        # Align exactly with consumer's encoded get_camera_identifier grammar.
        # It is an alias segment, not a filesystem path or a native-camera URL.
        host = config["sink_host"]
        if not isinstance(host, str):
            raise WorkerError("config_sink")
        try:
            address = ipaddress.IPv4Address(host)
        except ipaddress.AddressValueError:
            raise WorkerError("config_sink") from None
        if str(address) != host or address.is_unspecified or address.is_multicast:
            raise WorkerError("config_sink")
        port = config["rtsp_port"]
        if type(port) is not int or not 1024 <= port <= 65535:
            raise WorkerError("config_sink")
        if config["publisher_user"] != "window_publisher":
            raise WorkerError("config_user")
        password = config["publisher_password"]
        if not isinstance(password, str) or not _PASSWORD.fullmatch(password):
            raise WorkerError("config_password")
        # Exact host admission is the parent's configured local bind IP. This
        # validator rejects DNS/IPv6/coercions, but does not invent local routing.
        return config
    except WorkerError:
        raise
    except Exception:
        raise WorkerError("config_read") from None


def publish_url(config):
    """Build the secret-bearing URL only in child memory, never emit it."""
    return (f"rtsp://window_publisher:{config['publisher_password']}"
            f"@{config['sink_host']}:{config['rtsp_port']}/standby")


def configure_encoder(container):
    stream = container.add_stream("libx264", rate=RATE)
    stream.width, stream.height = SIZE
    stream.pix_fmt = "yuv420p"
    stream.time_base = Fraction(1, RATE)
    stream.codec_context.time_base = Fraction(1, RATE)
    stream.codec_context.thread_count = 1
    stream.codec_context.gop_size = 10
    stream.codec_context.max_b_frames = 0
    stream.options = {
        "profile": "main", "preset": "veryfast", "tune": "zerolatency",
        "x264-params": "keyint=10:min-keyint=10:scenecut=0:bframes=0:force-cfr=1",
    }
    return stream


def normalization_graph(av, template):
    graph = av.filter.Graph()
    graph.threads = 1
    source = graph.add_buffer(template=template)
    scale = graph.add("scale", "1920:1080:force_original_aspect_ratio=decrease:force_divisible_by=2")
    pad = graph.add("pad", "1920:1080:(ow-iw)/2:(oh-ih)/2:black")
    sar = graph.add("setsar", "1")
    fps = graph.add("fps", "fps=30:start_time=0:round=near")
    pixel_format = graph.add("format", "pix_fmts=yuv420p")
    sink = graph.add("buffersink")
    graph.link_nodes(source, scale, pad, sar, fps, pixel_format, sink)
    graph.configure()
    return graph


def run_pipeline(source_url, sink_url, should_stop, progress):
    """One capture, no retries. Progress is emitted only AFTER successful mux.

    Generic URLs here permit hermetic localhost fixtures; the deployed CLI only
    accepts HA's loopback alias plus a parent-pinned MediaMTX sink. A merely
    configured go2rtc alias is NOT proof that its upstream is already live: the
    parent must hold the real WebRTC consumer and receive a frame before launch.
    """
    av = silent_av()
    source_url, sink_url = validate_url(source_url), validate_url(sink_url)
    source = output = graph = None
    frames = packets = 0
    stage = "source_open"
    try:
        # Verified HA RTSP first decoded frames needed more than one second;
        # bound reads to four seconds, retaining a one-second connect timeout.
        # The independent parent watchdog still owns the full export deadline.
        source = av.open(source_url, "r", format="rtsp", timeout=(1.0, 4.0),
                         options={"rtsp_transport": "tcp", "allowed_media_types": "video"})
        videos = list(source.streams.video)
        if len(videos) != 1:
            raise WorkerError("video_track_count")
        input_stream = videos[0]
        input_stream.codec_context.thread_count = 1
        stage = "output_open"
        output = av.open(sink_url, "w", format="rtsp", timeout=(1.0, 1.0),
                         options={"rtsp_transport": "tcp"})
        stage = "output_encoder_config"
        encoded = configure_encoder(output)
        first_input_pts = previous_input_pts = None
        input_time_base = None
        missing_pts = 0

        def staged_items(items, operation):
            """Restore the operation before each potentially blocking next()."""
            nonlocal stage
            stage = operation
            iterator = iter(items)
            while True:
                stage = operation
                try:
                    item = next(iterator)
                except StopIteration:
                    return
                yield item

        def emit(filtered):
            nonlocal frames, packets, stage
            if should_stop():
                return False
            stage = "output_encode"
            filtered.pts = frames
            filtered.time_base = Fraction(1, RATE)
            current = frames
            frames += 1
            muxed = False
            produced = encoded.encode(filtered)
            for packet in staged_items(produced, "output_encode"):
                if should_stop():
                    return False
                stage = "output_mux"
                output.mux(packet)
                packets += 1
                muxed = True
            if muxed:
                stage = "progress_emit"
                progress({"event": "progress", "frame": frames, "packets": packets,
                          "out_time_us": (current + 1) * 1000000 // RATE})
            return True

        def drain():
            nonlocal stage
            while not should_stop():
                try:
                    stage = "filter_pull"
                    filtered = graph.pull()
                except (av.error.BlockingIOError, av.error.EOFError):
                    return True
                if not emit(filtered):
                    return False
            return False

        stage = "source_demux"
        incoming = source.demux(input_stream)
        for packet in staged_items(incoming, "source_demux"):
            if should_stop():
                break
            stage = "input_decode"
            decoded = packet.decode()
            for frame in staged_items(decoded, "input_decode"):
                if should_stop():
                    break
                if frame.pts is None or frame.time_base is None:
                    if missing_pts < 30:
                        # Skip up to 30 total omissions, as the local synthetic
                        # RTSP fixture required. Do not invent source timestamps.
                        missing_pts += 1
                        continue
                    raise WorkerError("input_timestamp_missing")
                if type(frame.pts) is not int or frame.time_base <= 0:
                    raise WorkerError("input_timestamp_invalid")
                if first_input_pts is None:
                    first_input_pts = frame.pts
                    input_time_base = frame.time_base
                if frame.time_base != input_time_base:
                    raise WorkerError("input_timestamp_changed")
                if previous_input_pts is not None and frame.pts < previous_input_pts:
                    raise WorkerError("input_timestamp_regressed")
                previous_input_pts = frame.pts
                frame.pts -= first_input_pts
                if graph is None:
                    stage = "filter_create"
                    graph = normalization_graph(av, frame)
                stage = "filter_push"
                graph.push(frame)
                if not drain():
                    break
        # No buffered frame flushing after the parent closes the export window.
        if graph is not None and not should_stop():
            stage = "filter_push"
            graph.push(None)
            drain()
        if not should_stop():
            stage = "output_flush"
            flushed = encoded.encode(None)
            for packet in staged_items(flushed, "output_flush"):
                stage = "output_mux"
                output.mux(packet)
                packets += 1
        return {"event": "finished", "frames": frames, "packets": packets,
                "outcome": "stopped" if should_stop() else "eof"}
    except WorkerError:
        raise
    except Exception:
        raise WorkerError("media_pipeline_" + stage) from None
    finally:
        for container in (output, source):
            if container is not None:
                try:
                    container.close()
                except Exception:
                    pass


def emit_error(code):
    """Exactly one safe protocol line, never stderr/raw exception strings."""
    if type(code) is not str or code not in WORKER_ERROR_CODES:
        code = "worker_internal"
    try:
        print(json.dumps({"error": code}), flush=True)
    except Exception:
        pass  # A closed owning pipe must not cause a traceback or fallback log.


def main():
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    if len(sys.argv) != 1:
        emit_error("arguments")
        return 2
    try:
        config = stdin_config(sys.stdin.buffer)
        run_pipeline(config["source"], publish_url(config), stop.is_set,
                     lambda data: print(json.dumps({"frames": data["frame"]}), flush=True))
        return 0
    except WorkerError as error:
        emit_error(error.args[0] if len(error.args) == 1 else "worker_internal")
        return 1
    except Exception:
        emit_error("worker_internal")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
