"""Media worker input, encoder, pipeline, and progress tests: no network."""
import importlib.util
import io
import json
from fractions import Fraction
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location("media_worker_test_subject", Path(__file__).with_name("media_worker.py"))
worker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(worker)


class ConfigTests(unittest.TestCase):
    def payload(self, **changes):
        data = {"source": "rtsp://127.0.0.1:18554/camera_fixture_123", "sink_host": "192.0.2.10",
                "rtsp_port": 18557, "publisher_user": "window_publisher", "publisher_password": "a" * 64}
        data.update(changes)
        return json.dumps(data).encode() + b"\n"

    def rejected(self, raw):
        with self.assertRaises(worker.WorkerError) as error:
            worker.stdin_config(io.BytesIO(raw))
        self.assertNotIn("rtsp", str(error.exception))
        self.assertNotIn("a" * 64, str(error.exception))

    def test_valid_generic_encoded_ha_alias(self):
        for alias in ("camera_fixture_123", "platform_id%3A123", "x" * 1024):
            data = worker.stdin_config(io.BytesIO(self.payload(source="rtsp://127.0.0.1:18554/" + alias)))
            self.assertTrue(data["source"].endswith(alias))

    def test_native_and_external_source_forbidden(self):
        for source in ("rtsp://192.0.2.20:6554/doorbell", "rtsp://localhost:18554/camera",
                       "rtsp://[::1]:18554/camera", "rtsp://192.0.2.10:18554/camera",
                       "rtsp://127.0.0.1:18555/camera"):
            self.rejected(self.payload(source=source))

    def test_source_userinfo_query_fragment_slash_and_bad_encoding_forbidden(self):
        for source in ("rtsp://u:p@127.0.0.1:18554/camera", "rtsp://127.0.0.1:18554/camera?x=1",
                       "rtsp://127.0.0.1:18554/camera#x", "rtsp://127.0.0.1:18554/camera/other",
                       "rtsp://127.0.0.1:18554/%XZ", "rtsp://127.0.0.1:18554/" + "x" * 1025):
            self.rejected(self.payload(source=source))

    def test_sink_strict_ipv4_only(self):
        for host in ("localhost", "example.com", "[::1]", "::1", "0.0.0.0", "224.0.0.1",
                     "127.000.0.1", "127.0.0.1:9000", 123, None):
            self.rejected(self.payload(sink_host=host))

    def test_sink_port_int_not_bool(self):
        for port in (True, False, 1023, 65536, "18557", 18557.0, None):
            self.rejected(self.payload(rtsp_port=port))

    def test_sink_loopback_or_valid_ipv4_and_ports_accepted(self):
        for host in ("127.0.0.1", "192.0.2.10"):
            for port in (1024, 18557, 65535):
                worker.stdin_config(io.BytesIO(self.payload(sink_host=host, rtsp_port=port)))

    def test_private_fixed_publisher_user(self):
        for user in ("protect", "arbitrary", "window_publisher:secret", None):
            self.rejected(self.payload(publisher_user=user))

    def test_password_exact_generated_alnum_64(self):
        for password in ("a" * 63, "a" * 65, "A" * 64, "a" * 63 + "!", "a" * 63 + "\n", None):
            self.rejected(self.payload(publisher_password=password))

    def test_exact_fields_missing_extra_duplicate(self):
        self.rejected(self.payload(extra="no"))
        data = json.loads(self.payload())
        del data["sink_host"]
        self.rejected(json.dumps(data).encode() + b"\n")
        self.rejected(self.payload().replace(b"{", b'{"source":"ignored",', 1))

    def test_utf8_bound_newline_scalar(self):
        for raw in (b"true\n", b"{\n", b"x" * 8193 + b"\n", self.payload().rstrip(b"\n"),
                    self.payload().decode().encode("utf-16")):
            self.rejected(raw)

    def test_publish_url_remains_memory_only(self):
        config = worker.stdin_config(io.BytesIO(self.payload()))
        self.assertEqual(worker.publish_url(config),
                         "rtsp://window_publisher:" + "a" * 64 + "@192.0.2.10:18557/standby")

    def test_versions_exact_without_dependency_downgrade(self):
        self.assertEqual(worker.VERSIONS, {"17.0.1", "19.0.1"})

    def test_encoder_parameters(self):
        encoder = SimpleNamespace(codec_context=SimpleNamespace(), options={})
        container = mock.Mock(add_stream=mock.Mock(return_value=encoder))
        worker.configure_encoder(container)
        container.add_stream.assert_called_once_with("libx264", rate=30)
        self.assertEqual((encoder.width, encoder.height), (1920, 1080))
        self.assertEqual(encoder.pix_fmt, "yuv420p")
        self.assertEqual(encoder.codec_context.gop_size, 10)
        self.assertEqual(encoder.codec_context.max_b_frames, 0)


class PipelineTests(unittest.TestCase):
    def pipeline(self, pts, fail_mux=False, stop_after_first=False, bases=None):
        class Blocked(Exception): pass
        class End(Exception): pass
        class Graph:
            def __init__(self): self.frames = []
            def push(self, frame):
                if frame is not None: self.frames.append(frame)
            def pull(self):
                if not self.frames: raise Blocked()
                return self.frames.pop(0)
        frames = [SimpleNamespace(pts=value, time_base=Fraction(1, 90000) if bases is None else bases[i])
                  for i, value in enumerate(pts)]
        source = mock.Mock()
        source.streams.video = [SimpleNamespace(codec_context=SimpleNamespace(thread_count=0))]
        source.demux.return_value = [SimpleNamespace(decode=lambda frame=frame: [frame]) for frame in frames]
        output = mock.Mock()
        if fail_mux: output.mux.side_effect = RuntimeError("PRIVATE_URL_PASSWORD")
        fake_av = SimpleNamespace(open=mock.Mock(side_effect=[source, output]),
                                  error=SimpleNamespace(BlockingIOError=Blocked, EOFError=End))
        encoder = SimpleNamespace(encode=lambda frame: [] if frame is None else [object()])
        progress = []
        with (mock.patch.object(worker, "silent_av", return_value=fake_av),
              mock.patch.object(worker, "configure_encoder", return_value=encoder),
              mock.patch.object(worker, "normalization_graph", return_value=Graph())):
            try:
                result = worker.run_pipeline("rtsp://127.0.0.1:18554/camera",
                                             "rtsp://127.0.0.1:18557/standby",
                                             lambda: stop_after_first and output.mux.call_count >= 1,
                                             progress.append)
                return result, progress, output, fake_av
            finally:
                if fail_mux: self.assertEqual(progress, [])
                source.close.assert_called_once()
                output.close.assert_called_once()

    def test_progress_only_after_successful_mux(self):
        result, progress, output, av = self.pipeline([9000, 15000])
        self.assertEqual(result["packets"], 2)
        self.assertEqual([item["frame"] for item in progress], [1, 2])
        self.assertEqual(output.mux.call_count, 2)
        self.assertEqual(av.open.call_args_list[0].kwargs["options"]["allowed_media_types"], "video")
        self.assertEqual(av.open.call_args_list[0].kwargs["timeout"], (1.0, 1.0))

    def test_missing_pts_30_bound(self):
        result, progress, _, _ = self.pipeline([9000] + [None] * 30 + [195000])
        self.assertEqual(result["frames"], 2)
        self.assertEqual(len(progress), 2)

    def test_missing_pts_31_fails_closed(self):
        with self.assertRaises(worker.WorkerError) as caught:
            self.pipeline([9000] + [None] * 31)
        self.assertEqual(str(caught.exception), "input_timestamp_missing")

    def test_timestamp_regression_fails_closed(self):
        with self.assertRaises(worker.WorkerError) as caught:
            self.pipeline([9000, 15000, 12000])
        self.assertEqual(str(caught.exception), "input_timestamp_regressed")

    def test_time_base_change_fails_closed(self):
        with self.assertRaises(worker.WorkerError) as caught:
            self.pipeline([9000, 15000], bases=[Fraction(1, 90000), Fraction(1, 1000)])
        self.assertEqual(str(caught.exception), "input_timestamp_changed")

    def test_mux_error_redacted_no_progress(self):
        with self.assertRaises(worker.WorkerError) as caught:
            self.pipeline([9000], fail_mux=True)
        self.assertEqual(str(caught.exception), "media_pipeline_decode_filter_mux")

    def test_stop_no_buffer_flush(self):
        result, progress, output, _ = self.pipeline([9000, 15000, 21000], stop_after_first=True)
        self.assertEqual(result["outcome"], "stopped")
        self.assertEqual(output.mux.call_count, 1)
        self.assertEqual(len(progress), 1)


@unittest.skipUnless(importlib.util.find_spec("av"), "optional existing local PyAV runtime")
class RealCodecTests(unittest.TestCase):
    def test_real_encoder_filter_and_decode_in_memory_no_sockets(self):
        av = worker.silent_av()
        buffer = io.BytesIO()
        output = av.open(buffer, "w", format="mp4")
        encoder = worker.configure_encoder(output)
        first = av.VideoFrame(640, 480, "yuv420p")
        first.pts, first.time_base = 0, Fraction(1, 15)
        graph = worker.normalization_graph(av, first)
        count = 0
        for index in range(4):
            frame = av.VideoFrame(640, 480, "yuv420p")
            frame.pts, frame.time_base = index, Fraction(1, 15)
            graph.push(frame)
            while True:
                try:
                    filtered = graph.pull()
                except (av.error.BlockingIOError, av.error.EOFError):
                    break
                filtered.pts, filtered.time_base = count, Fraction(1, 30)
                count += 1
                for packet in encoder.encode(filtered):
                    output.mux(packet)
        for packet in encoder.encode(None):
            output.mux(packet)
        output.close()
        source = av.open(io.BytesIO(buffer.getvalue()), "r")
        try:
            decoded = list(source.decode(video=0))
            self.assertGreater(len(decoded), 0)
            self.assertEqual(len(decoded), count)
            self.assertEqual((decoded[0].width, decoded[0].height), (1920, 1080))
            self.assertEqual(decoded[0].format.name, "yuv420p")
            self.assertEqual(source.streams.video[0].codec_context.profile, "Main")
            self.assertEqual(source.streams.video[0].average_rate, 30)
        finally:
            source.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
