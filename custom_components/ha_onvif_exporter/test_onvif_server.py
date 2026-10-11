"""Public fixtures only. Real aiohttp tests listen only on IPv4 loopback."""
from __future__ import annotations

import asyncio
import base64
import hashlib
import importlib.util
import json
import socket
import sys
import time
import unittest
from unittest import mock
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

from aiohttp import ClientSession
from aiohttp.test_utils import TestClient, TestServer

spec = importlib.util.spec_from_file_location("tested_onvif", Path(__file__).with_name("onvif_server.py"))
m = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = m
spec.loader.exec_module(m)
USER, PASSWORD = "public_test_user", "PUBLIC_TEST_PASSWORD_0123456789"
BASIC = "Basic " + base64.b64encode((USER + ":" + PASSWORD).encode()).decode()


def config(**extra):
    values = dict(bind_host="127.0.0.1", port=1986, advertised_host="127.0.0.1",
                  rtsp_uri="rtsp://127.0.0.1:18557/standby", username=USER,
                  password=PASSWORD, device_id="public-entry-camera",
                  allowed_peers={"127.0.0.1"})
    values.update(extra)
    return values


def op(namespace, name, fields=()):
    node = ET.Element(m.q(namespace, name))
    for ns, key, value in fields:
        m.add(node, ns, key, value)
    return node


def soap(operation, *, version=m.SOAP12, ws=False, nonce=None, created=None,
         user=USER, password=PASSWORD, bare=True, mutate=None):
    root = ET.Element(m.q(version, "Envelope"))
    if ws:
        header = m.add(root, version, "Header")
        attributes = {"mustUnderstand" if bare else m.q(version, "mustUnderstand"): "1"}
        security = m.add(header, m.WSSE, "Security", **attributes)
        token = m.add(security, m.WSSE, "UsernameToken")
        nonce = nonce or time.time_ns().to_bytes(16, "big")
        created = created or datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
        digest = base64.b64encode(hashlib.sha1(nonce + created.encode() + password.encode()).digest()).decode()
        m.add(token, m.WSSE, "Username", user)
        m.add(token, m.WSSE, "Password", digest, Type=m.PASSWORD_DIGEST)
        m.add(token, m.WSSE, "Nonce", base64.b64encode(nonce).decode(), EncodingType=m.BASE64_BINARY)
        m.add(token, m.WSU, "Created", created)
    body = m.add(root, version, "Body")
    body.append(operation)
    if mutate:
        mutate(root)
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def stream_op(token="standby", protocol="RTSP"):
    node = op(m.MEDIA, "GetStreamUri")
    setup = m.add(node, m.MEDIA, "StreamSetup")
    m.add(setup, m.SCHEMA, "Stream", "RTP-Unicast")
    m.add(m.add(setup, m.SCHEMA, "Transport"), m.SCHEMA, "Protocol", protocol)
    m.add(node, m.MEDIA, "ProfileToken", token)
    return node


class ConfigurationTests(unittest.TestCase):
    def test_stable_identity_and_compat_tokens(self):
        a, b = m.ONVIFServer(**config()), m.ONVIFServer(**config())
        self.assertEqual(a.mac_address, b.mac_address)
        self.assertEqual(a.device_uuid, b.device_uuid)
        self.assertEqual((a.profile_token, a.encoder_token), ("standby", "vec"))
        self.assertNotEqual(a.device_uuid, m.ONVIFServer(**config(device_id="another-camera")).device_uuid)

    def test_bad_configurations(self):
        fixtures = [dict(allowed_peers=set()), dict(allowed_peers={"0.0.0.0/0"}),
                    dict(allowed_peers={"example.org"}), dict(bind_host="localhost"),
                    dict(port=0), dict(port=True), dict(password="short"),
                    dict(username="user:admin"), dict(password="a" * 31 + "\n"),
                    dict(profile_token="../secret"), dict(encoder_token=".."),
                    dict(mac_address="not-mac"), dict(rtsp_uri="rtsp://192.0.2.20:6554/doorbell-video"),
                    dict(rtsp_uri="http://127.0.0.1:18557/standby"),
                    dict(rtsp_uri="rtsp://u:p@127.0.0.1:18557/standby"),
                    dict(rtsp_uri="rtsp://127.0.0.1:18557/standby?native=1"),
                    dict(rtsp_uri="rtsp://127.0.0.1:18557/standby#fragment"),
                    dict(rtsp_uri="rtsp://127.0.0.1:18557/.."),
                    dict(rtsp_uri="rtsp://127.0.0.1:0/standby")]
        for fixture in fixtures:
            with self.subTest(fixture=next(iter(fixture))):
                with self.assertRaisesRegex(ValueError, "invalid_onvif_configuration"):
                    m.ONVIFServer(**config(**fixture))

    def test_basic_canonical_and_constant_credential(self):
        server = m.ONVIFServer(**config())
        self.assertTrue(server._authorized_basic(BASIC))
        for value in (None, "", BASIC + "\n", BASIC + "=", "Digest abc", "Basic !!!", "basic " + BASIC[6:]):
            self.assertFalse(server._authorized_basic(value))


class XMLAndWSTests(unittest.TestCase):
    def validate(self, data, validator=None):
        root, namespace, _ = m.parse_envelope(data)
        (validator or m.UsernameTokenValidator(USER, PASSWORD)).validate(root, namespace)

    def test_valid_and_replay(self):
        data = soap(op(m.DEVICE, "GetServices"), ws=True)
        validator = m.UsernameTokenValidator(USER, PASSWORD)
        self.validate(data, validator)
        with self.assertRaises(m.Rejected):
            self.validate(data, validator)

    def test_protect_bare_must_understand_closed_subset(self):
        self.validate(soap(op(m.DEVICE, "GetServices"), ws=True))
        for value in ("0", "true", "", "unknown"):
            def change(root, value=value):
                root.find(".//" + m.q(m.WSSE, "Security")).set("mustUnderstand", value)
            with self.subTest(value=value), self.assertRaises(m.Rejected):
                self.validate(soap(op(m.DEVICE, "GetServices"), ws=True, mutate=change))
        with self.assertRaises(m.Rejected):
            self.validate(soap(op(m.DEVICE, "GetServices"), ws=True, version=m.SOAP11))
        self.validate(soap(op(m.DEVICE, "GetServices"), ws=True, version=m.SOAP11, bare=False))
        def dual(root):
            root.find(".//" + m.q(m.WSSE, "Security")).set(m.q(m.SOAP12, "mustUnderstand"), "1")
        with self.assertRaises(m.Rejected):
            self.validate(soap(op(m.DEVICE, "GetServices"), ws=True, mutate=dual))

    def test_invalid_user_password_and_timestamps(self):
        now = time.time()
        for kwargs in (dict(user="not-public-user"), dict(password="NOT_THE_PUBLIC_PASSWORD_0123456789"),
                       dict(created=m._utc(now - 301)), dict(created=m._utc(now + 31)),
                       dict(created="2026-10-11T12:00:00+00:00"), dict(created="2026-99-11T12:00:00Z")):
            with self.subTest(case=next(iter(kwargs))), self.assertRaises(m.Rejected):
                self.validate(soap(op(m.DEVICE, "GetServices"), ws=True, **kwargs))

    def test_cache_capacity_and_failed_auth_does_not_fill(self):
        validator = m.UsernameTokenValidator(USER, PASSWORD, limit=1)
        bad = soap(op(m.DEVICE, "GetServices"), ws=True, password="WRONG_PUBLIC_PASSWORD_0123456789")
        with self.assertRaises(m.Rejected):
            self.validate(bad, validator)
        self.assertEqual(len(validator._seen), 0)
        self.validate(soap(op(m.DEVICE, "GetServices"), ws=True), validator)
        with self.assertRaises(m.Rejected):
            self.validate(soap(op(m.DEVICE, "GetServices"), ws=True), validator)

    def test_clock_regression_nan_and_cache_retention(self):
        wall, mono = [time.time()], [1000.0]
        validator = m.UsernameTokenValidator(USER, PASSWORD, clock=lambda: wall[0], monotonic=lambda: mono[0])
        created = m._utc(wall[0])
        self.validate(soap(op(m.DEVICE, "GetServices"), ws=True, created=created), validator)
        wall[0] -= 1
        with self.assertRaises(m.Rejected):
            self.validate(soap(op(m.DEVICE, "GetServices"), ws=True, created=created), validator)
        wall[0] = float("nan")
        with self.assertRaises(m.Rejected):
            self.validate(soap(op(m.DEVICE, "GetServices"), ws=True, created=created), validator)

    def test_base64_nonce_type_shape(self):
        mutations = [
            lambda r: r.find(".//" + m.q(m.WSSE, "Nonce")).__setattr__("text", "!!!"),
            lambda r: r.find(".//" + m.q(m.WSSE, "Nonce")).__setattr__("text", ""),
            lambda r: r.find(".//" + m.q(m.WSSE, "Password")).set("Type", "PasswordText"),
            lambda r: r.find(".//" + m.q(m.WSSE, "Nonce")).set("EncodingType", "unknown"),
            lambda r: m.add(r.find(".//" + m.q(m.WSSE, "Username")), m.WSSE, "Nested", "value"),
            lambda r: m.add(r.find(".//" + m.q(m.WSSE, "UsernameToken")), m.WSSE, "Username", USER),
            lambda r: m.add(r.find(".//" + m.q(m.WSSE, "Security")), m.WSSE, "UsernameToken"),
            lambda r: r.find(".//" + m.q(m.WSSE, "Security")).set("actor", "any"),
        ]
        for index, mutation in enumerate(mutations):
            with self.subTest(index=index), self.assertRaises(m.Rejected):
                self.validate(soap(op(m.DEVICE, "GetServices"), ws=True, mutate=mutation))

    def test_xml_bombs_namespace_wrapping_and_bounds(self):
        fixtures = [b"", b"x" * (m.MAX_BODY + 1), b"\xff", b"<x>\x00</x>",
                    b'<!DOCTYPE x [<!ENTITY x SYSTEM "file:///never-read">]><x>&x;</x>',
                    b'<!DOCTYPE x [<!ENTITY x "abc">]><x>&x;</x>',
                    b'<Envelope><Body><x/></Body></Envelope>',
                    soap(op(m.DEVICE, "GetServices"), mutate=lambda r: m.add(r, m.SOAP12, "Body")),
                    soap(op(m.DEVICE, "GetServices"), mutate=lambda r: m.add(r[-1], m.DEVICE, "GetProfiles")),
                    soap(op(m.DEVICE, "GetServices"), mutate=lambda r: m.add(r[-1][0], "evil", "Body")),
                    soap(op(m.DEVICE, "GetServices")).replace(b"utf-8", b"utf-16"),
                    b'<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope"><s:Body><x>' + b'<a>'*40 + b'</a>'*40 + b'</x></s:Body></s:Envelope>',
                    b'<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope"><s:Body><x>' + b'<a/>'*1025 + b'</x></s:Body></s:Envelope>']
        for index, fixture in enumerate(fixtures):
            with self.subTest(index=index), self.assertRaises(m.Rejected):
                m.parse_envelope(fixture)


class HTTPTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.server = m.ONVIFServer(**config())
        self.client = TestClient(TestServer(self.server.app, host="127.0.0.1"))
        await self.client.start_server()

    async def asyncTearDown(self):
        await self.server.stop()
        await self.client.close()

    async def post(self, operation, *, auth=BASIC, path="/onvif/device_service", ws=False, raw=None, headers=None):
        values = {"Content-Type": "application/soap+xml"}
        if auth is not None:
            values["Authorization"] = auth
        values.update(headers or {})
        response = await self.client.post(path, data=raw or soap(operation, ws=ws), headers=values)
        data = await response.read()
        return response.status, ET.fromstring(data)

    async def subscription(self, lease="PT60S"):
        operation = op(m.EVENTS, "CreatePullPointSubscription", [(m.EVENTS, "InitialTerminationTime", lease)])
        status, root = await self.post(operation, path="/onvif/events_service")
        self.assertEqual(status, 200)
        address = root.find(".//" + m.q(m.WSA, "Address")).text
        return "/onvif/subscription/" + address.rsplit("/", 1)[-1]

    async def pull(self, path, timeout="PT0S", limit="64"):
        return await self.post(op(m.EVENTS, "PullMessages", [(m.EVENTS, "Timeout", timeout), (m.EVENTS, "MessageLimit", limit)]), path=path)

    async def test_public_clock_only_and_uniform_auth_failure(self):
        status, root = await self.post(op(m.DEVICE, "GetSystemDateAndTime"), auth=None)
        self.assertEqual(status, 200)
        self.assertIsNotNone(root.find(".//" + m.q(m.SCHEMA, "UTCDateTime")))
        for operation, auth in ((op(m.DEVICE, "GetServices"), None),
                                (op(m.DEVICE, "GetSystemDateAndTime"), "Basic bad"),
                                (op(m.MEDIA, "GetProfiles"), "Basic bad")):
            status, root = await self.post(operation, auth=auth)
            self.assertEqual(status, 401)
            self.assertNotIn(PASSWORD, ET.tostring(root).decode())

    async def test_ws_protect_shape_and_replay(self):
        data = soap(op(m.DEVICE, "GetServices"), ws=True)
        self.assertEqual((await self.post(None, raw=data, auth=None))[0], 200)
        self.assertEqual((await self.post(None, raw=data, auth=None))[0], 401)
        # Invalid WS cannot fall back even with valid Basic.
        data = soap(op(m.DEVICE, "GetServices"), ws=True, password="WRONG_PUBLIC_PASSWORD_0123456789")
        self.assertEqual((await self.post(None, raw=data))[0], 401)

    async def test_addressing_response_uses_authenticated_tree_without_callbacks(self):
        def addressing(root):
            header = root.find(m.q(m.SOAP12, "Header"))
            m.add(header, m.WSA, "Action", m.EVENTS + "/EventPortType/GetEventPropertiesRequest")
            m.add(header, m.WSA, "MessageID", "urn:uuid:00000000-0000-0000-0000-000000000001")
            m.add(m.add(header, m.WSA, "ReplyTo"), m.WSA, "Address", "http://never-follow.invalid/callback")
        data = soap(op(m.EVENTS, "GetEventProperties"), ws=True, mutate=addressing)
        status, root = await self.post(None, raw=data, auth=None)
        self.assertEqual(status, 200)
        self.assertEqual(root.find(".//" + m.q(m.WSA, "Action")).text, m.EVENTS + "/EventPortType/GetEventPropertiesResponse")
        self.assertEqual(root.find(".//" + m.q(m.WSA, "RelatesTo")).text, "urn:uuid:00000000-0000-0000-0000-000000000001")
        def wrong_action(root):
            addressing(root)
            root.find(".//" + m.q(m.WSA, "Action")).text = m.DEVICE + "/SystemReboot"
        self.assertEqual((await self.post(None, raw=soap(op(m.EVENTS, "GetEventProperties"), ws=True, mutate=wrong_action), auth=None))[0], 400)

    async def test_pull_cancellation_releases_flag_and_preserves_pending_state(self):
        path = await self.subscription()
        await self.pull(path)
        key = path.rsplit("/", 1)[-1]
        operation = op(m.EVENTS, "PullMessages", [(m.EVENTS, "Timeout", "PT30S"), (m.EVENTS, "MessageLimit", "1")])
        task = asyncio.create_task(self.server._subscription_dispatch(operation, "127.0.0.1", key))
        await asyncio.sleep(0.01)
        self.assertTrue(self.server._subscriptions[key].pulling)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertFalse(self.server._subscriptions[key].pulling)
        self.server.set_motion(True)
        self.assertEqual((await self.pull(path))[0], 200)

    async def test_services_capabilities_properties_and_all_responses_valid_xml(self):
        for operation in (op(m.DEVICE, "GetServices", [(m.DEVICE, "IncludeCapability", "true")]),
                          op(m.DEVICE, "GetCapabilities"), op(m.DEVICE, "GetServiceCapabilities"),
                          op(m.EVENTS, "GetServiceCapabilities"), op(m.EVENTS, "GetEventProperties"),
                          op(m.MEDIA, "GetServiceCapabilities"), op(m.MEDIA, "GetProfiles"),
                          op(m.MEDIA, "GetVideoSources"), op(m.MEDIA, "GetAudioSources"),
                          op(m.DEVICE, "GetNetworkInterfaces"), op(m.DEVICE, "GetScopes")):
            status, root = await self.post(operation)
            self.assertEqual(status, 200)
            self.assertNotIn(PASSWORD, ET.tostring(root).decode())
        status, root = await self.post(op(m.MEDIA, "GetProfiles"))
        profile = root.find(".//" + m.q(m.MEDIA, "Profiles"))
        self.assertEqual(profile.get("token"), "standby")
        self.assertEqual(profile.find(m.q(m.SCHEMA, "VideoEncoderConfiguration")).get("token"), "vec")

    async def test_stream_uri_pinned_no_arbitrary_url_or_wrong_tokens(self):
        status, root = await self.post(stream_op(), path="/onvif/media_service")
        self.assertEqual(status, 200)
        self.assertEqual(root.find(".//" + m.q(m.SCHEMA, "Uri")).text, "rtsp://127.0.0.1:18557/standby")
        self.assertEqual(root.find(".//" + m.q(m.SCHEMA, "Timeout")).text, "PT0S")
        for operation in (stream_op("another"), stream_op(protocol="UDP"),
                          op(m.MEDIA, "GetVideoEncoderConfiguration", [(m.MEDIA, "ConfigurationToken", "standby")]),
                          op(m.MEDIA, "GetProfile", [(m.MEDIA, "ProfileToken", "http://evil.invalid")]),
                          op(m.MEDIA, "GetProfiles", [(m.MEDIA, "URL", "file:///never-read")])):
            self.assertEqual((await self.post(operation))[0], 400)

    async def test_no_admin_actions_callbacks_discovery_or_mutators(self):
        for namespace, name in ((m.DEVICE, "SystemReboot"), (m.DEVICE, "SetSystemDateAndTime"),
                                (m.DEVICE, "SetNetworkInterfaces"), (m.DEVICE, "SetUsers"),
                                (m.MEDIA, "SetVideoEncoderConfiguration"), (m.WSNT, "Subscribe"),
                                (m.EVENTS, "AddEventBroker"), ("http://evil.invalid", "GetProfiles")):
            self.assertEqual((await self.post(op(namespace, name)))[0], 400)
        response = await self.client.get("/onvif/device_service")
        self.assertEqual(response.status, 405)
        response = await self.client.post("/unconfigured", data=b"test")
        self.assertEqual(response.status, 404)

    async def test_bad_transport_query_compression_duplicate_field(self):
        for headers in ({"Content-Type": "application/json"}, {"Content-Encoding": "gzip"}):
            self.assertEqual((await self.post(op(m.DEVICE, "GetServices"), headers=headers))[0], 400)
        self.assertEqual((await self.post(op(m.DEVICE, "GetServices"), path="/onvif/device_service?url=x"))[0], 400)
        operation = op(m.MEDIA, "GetProfile", [(m.MEDIA, "ProfileToken", "standby"), (m.MEDIA, "ProfileToken", "standby")])
        self.assertEqual((await self.post(operation))[0], 400)
        self.assertEqual((await self.post(None, raw=b"x" * (m.MAX_BODY + 1)))[0], 400)

    async def test_peer_acl_no_proxy_header_override(self):
        self.server.allowed_peers = frozenset({"192.0.2.1"})
        self.assertEqual((await self.post(op(m.DEVICE, "GetSystemDateAndTime"), headers={"X-Forwarded-For": "192.0.2.1"}))[0], 403)

    async def test_motion_initial_true_false_only_after_set_motion(self):
        path = await self.subscription()
        _, root = await self.pull(path)
        item = root.find(".//" + m.q(m.SCHEMA, "Message"))
        self.assertEqual(item.get("PropertyOperation"), "Initialized")
        self.assertEqual(item.find(".//" + m.q(m.SCHEMA, "Data") + "/" + m.q(m.SCHEMA, "SimpleItem")).get("Value"), "false")
        self.server.set_motion(True)
        self.server.set_motion(True)  # No duplicate state change.
        self.server.set_motion(False)
        _, root = await self.pull(path)
        values = [e.get("Value") for e in root.iter(m.q(m.SCHEMA, "SimpleItem")) if e.get("Name") == "IsMotion"]
        self.assertEqual(values, ["true", "false"])
        _, root = await self.pull(path)
        self.assertEqual(len(list(root.iter(m.q(m.WSNT, "NotificationMessage")))), 0)

    async def test_pull_wakes_on_real_ready_and_renew_sync_unsubscribe(self):
        path = await self.subscription()
        await self.pull(path)
        waiter = asyncio.create_task(self.pull(path, timeout="PT2S"))
        await asyncio.sleep(0.02)
        self.server.set_motion(True)
        status, root = await waiter
        self.assertEqual(status, 200)
        self.assertEqual(root.find(".//" + m.q(m.SCHEMA, "Data") + "/" + m.q(m.SCHEMA, "SimpleItem")).get("Value"), "true")
        self.assertEqual((await self.post(op(m.WSNT, "Renew", [(m.WSNT, "TerminationTime", "PT20S")]), path=path))[0], 200)
        self.assertEqual((await self.post(op(m.EVENTS, "SetSynchronizationPoint"), path=path))[0], 200)
        _, root = await self.pull(path)
        self.assertEqual(root.find(".//" + m.q(m.SCHEMA, "Message")).get("PropertyOperation"), "Initialized")
        self.assertEqual((await self.post(op(m.WSNT, "Unsubscribe"), path=path))[0], 200)
        self.assertEqual((await self.pull(path))[0], 400)

    async def test_subscription_and_queue_limits_fail_closed(self):
        paths = [await self.subscription() for _ in range(m.MAX_PULL_POINTS)]
        self.assertEqual((await self.post(op(m.EVENTS, "CreatePullPointSubscription")))[0], 400)
        for index in range(m.MAX_MESSAGES + 5):
            self.server.set_motion(index % 2 == 0)
        for sub in self.server._subscriptions.values():
            self.assertLessEqual(len(sub.queue), m.MAX_MESSAGES)
            self.assertEqual(sub.queue[-1].enabled, self.server._motion)
        for timeout, limit in (("PT31S", "1"), ("P1D", "1"), ("PT1S", "65"), ("PT1S", "0"), ("PT1S", "NaN")):
            self.assertEqual((await self.pull(paths[0], timeout, limit))[0], 400)

    async def test_subscription_peer_binding_expiry_and_concurrent_pull(self):
        path = await self.subscription()
        key = path.rsplit("/", 1)[-1]
        await self.pull(path)
        waiter = asyncio.create_task(self.pull(path, timeout="PT0.15S"))
        await asyncio.sleep(0.02)
        self.assertEqual((await self.pull(path))[0], 400)
        self.assertEqual((await waiter)[0], 200)
        sub = self.server._subscriptions[key]
        sub.peer = "192.0.2.1"
        self.assertEqual((await self.pull(path))[0], 400)
        sub.peer = "127.0.0.1"
        sub.expires = self.server._monotonic() - 1
        self.assertEqual((await self.pull(path))[0], 400)

    async def test_snapshot_optional_auth_and_provider_failures_sanitized(self):
        operation = op(m.MEDIA, "GetSnapshotUri", [(m.MEDIA, "ProfileToken", "standby")])
        self.assertEqual((await self.post(operation))[0], 400)
        async def jpeg():
            return b"\xff\xd8PUBLIC_FIXTURE\xff\xd9"
        self.server._snapshot = jpeg
        status, root = await self.post(operation)
        self.assertEqual(status, 200)
        self.assertEqual(root.find(".//" + m.q(m.SCHEMA, "Uri")).text, "http://127.0.0.1:1986/onvif/snapshot/standby")
        response = await self.client.get("/onvif/snapshot/standby")
        self.assertEqual(response.status, 401)
        response = await self.client.get("/onvif/snapshot/standby", headers={"Authorization": BASIC})
        self.assertEqual(response.status, 200)
        async def failure():
            raise ValueError(PASSWORD)
        self.server._snapshot = failure
        response = await self.client.get("/onvif/snapshot/standby", headers={"Authorization": BASIC})
        self.assertEqual(response.status, 503)
        self.assertNotIn(PASSWORD, await response.text())

    async def test_stop_unblocks_pending_pull(self):
        path = await self.subscription()
        await self.pull(path)
        waiter = asyncio.create_task(self.pull(path, timeout="PT30S"))
        await asyncio.sleep(0.02)
        await self.server.stop()
        self.assertEqual((await asyncio.wait_for(waiter, 1))[0], 400)
        self.assertEqual(self.server._inflight, 0)

    async def test_request_counter_capacity(self):
        self.server._inflight = m.MAX_REQUESTS
        self.assertEqual((await self.post(op(m.DEVICE, "GetServices")))[0], 503)
        self.server._inflight = 0

    async def test_diagnostics_counts_only_successful_prepared_responses(self):
        self.assertEqual(self.server.diagnostics_snapshot()["operations_successful"]["media_get_profiles"], 0)
        self.assertEqual((await self.post(op(m.MEDIA, "GetProfiles"), auth=None))[0], 401)
        self.assertEqual((await self.post(op(m.MEDIA, "GetProfiles", [(m.MEDIA, "URL", "private_marker")])))[0], 400)
        self.assertEqual((await self.post(op("http://fixture.invalid/private_marker", "ArbitraryPrivateOperation")))[0], 400)
        self.assertEqual((await self.post(op(m.MEDIA, "GetProfiles")))[0], 200)
        snapshot = self.server.diagnostics_snapshot()
        self.assertEqual(snapshot["operations_successful"]["media_get_profiles"], 1)
        self.assertEqual(set(snapshot["operations_successful"]), set(m._DIAGNOSTIC_OPERATIONS.values()))
        text = json.dumps(snapshot)
        for forbidden in (PASSWORD, USER, "127.0.0.1", "private_marker", "ArbitraryPrivateOperation",
                          "rtsp:", "/onvif/", "public-entry-camera", "standby"):
            self.assertNotIn(forbidden, text)
        # Returned counters are a detached copy, not an administrative handle.
        snapshot["operations_successful"]["media_get_profiles"] = 99
        self.assertEqual(self.server.diagnostics_snapshot()["operations_successful"]["media_get_profiles"], 1)

    async def test_diagnostics_motion_notifications_counts_are_pull_response_counts(self):
        path = await self.subscription()
        before = self.server.diagnostics_snapshot()
        self.assertEqual(before["active_subscriptions"], 1)
        self.assertEqual(before["queued_notifications"], 1)
        self.assertEqual(before["pull_notifications_false"], 0)
        self.assertEqual(before["operations_successful"]["events_create_pull_point_subscription"], 1)
        await self.pull(path)
        self.server.set_motion(True)
        self.server.set_motion(True)
        self.server.set_motion(False)
        self.assertEqual(self.server.diagnostics_snapshot()["pull_notifications_true"], 0)
        await self.pull(path)
        await self.pull(path)  # Empty response is successful, but no notification.
        after = self.server.diagnostics_snapshot()
        self.assertEqual(after["pull_notifications_true"], 1)
        self.assertEqual(after["pull_notifications_false"], 2)
        self.assertEqual(after["operations_successful"]["events_pull_messages"], 3)
        self.assertFalse(after["is_motion"])
        self.assertEqual(after["queued_notifications"], 0)

    async def test_diagnostics_read_does_not_expire_renew_drain_or_signal(self):
        path = await self.subscription()
        key = path.rsplit("/", 1)[-1]
        sub = self.server._subscriptions[key]
        sub.expires = self.server._monotonic() - 1
        sub.changed.clear()
        before = (sub.expires, tuple(sub.queue), sub.closed, sub.pulling)
        with mock.patch.object(self.server, "_expire", side_effect=AssertionError("mutation")):
            snapshot = self.server.diagnostics_snapshot()
        self.assertEqual(snapshot["active_subscriptions"], 0)
        self.assertEqual(snapshot["queued_notifications"], 0)
        self.assertEqual((sub.expires, tuple(sub.queue), sub.closed, sub.pulling), before)
        self.assertFalse(sub.changed.is_set())
        self.assertIn(key, self.server._subscriptions)

    async def test_diagnostics_success_counter_saturates_and_failed_response_not_counted(self):
        self.server._operations_successful["media_get_profiles"] = m.MAX_DIAGNOSTIC_COUNT
        await self.post(op(m.MEDIA, "GetProfiles"))
        self.assertEqual(self.server.diagnostics_snapshot()["operations_successful"]["media_get_profiles"], m.MAX_DIAGNOSTIC_COUNT)
        self.server._operations_successful["media_get_profiles"] = 0
        original = self.server._response
        def fail_success(namespace, response, *args, **kwargs):
            if response.tag == m.q(m.MEDIA, "GetProfilesResponse"):
                raise ValueError("PRIVATE_ERROR_MUST_NOT_APPEAR")
            return original(namespace, response, *args, **kwargs)
        with mock.patch.object(self.server, "_response", side_effect=fail_success):
            status, root = await self.post(op(m.MEDIA, "GetProfiles"))
        self.assertEqual(status, 503)
        self.assertNotIn("PRIVATE_ERROR_MUST_NOT_APPEAR", ET.tostring(root).decode())
        self.assertEqual(self.server.diagnostics_snapshot()["operations_successful"]["media_get_profiles"], 0)

    async def test_diagnostics_pulling_subscription_visible_without_interrupting_pull(self):
        path = await self.subscription()
        await self.pull(path)
        waiter = asyncio.create_task(self.pull(path, timeout="PT0.15S"))
        await asyncio.sleep(0.02)
        snapshot = self.server.diagnostics_snapshot()
        self.assertEqual(snapshot["pulling_subscriptions"], 1)
        self.assertGreaterEqual(snapshot["inflight_requests"], 1)
        self.assertFalse(waiter.done())
        self.assertEqual((await waiter)[0], 200)
        self.assertEqual(self.server.diagnostics_snapshot()["pulling_subscriptions"], 0)


class ListenerLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_diagnostics_rejects_wrong_thread_after_start(self):
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        server = m.ONVIFServer(**config(port=port))
        try:
            await server.start()
            self.assertFalse(server.diagnostics_snapshot()["is_motion"])
            with self.assertRaises(RuntimeError):
                await asyncio.to_thread(server.diagnostics_snapshot)
        finally:
            await server.stop()

    async def test_start_stop_exact_loopback_listener(self):
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        server = m.ONVIFServer(**config(port=port))
        try:
            await server.start()
            with self.assertRaisesRegex(RuntimeError, "already_started"):
                await server.start()
            async with ClientSession() as client:
                async with client.post(f"http://127.0.0.1:{port}/onvif/device_service",
                                       data=soap(op(m.DEVICE, "GetSystemDateAndTime")),
                                       headers={"Content-Type": "application/soap+xml"}) as response:
                    self.assertEqual(response.status, 200)
        finally:
            await server.stop()
        self.assertIsNone(server._runner)
        self.assertEqual(len(server._subscriptions), 0)


if __name__ == "__main__":
    unittest.main()
