"""Authenticated, deliberately limited ONVIF Device/Media/PullPoint exporter.

This is an original application implementation, not a certified ONVIF device.
The UsernameToken validation derives from this project's audited ws_auth.py;
no external implementation source is incorporated. PasswordDigest authenticates
possession of the password, NOT the SOAP Body. Use the peer ACL and a trusted LAN
or TLS termination. The nonce cache is bounded, process-local and not durable.

No RTSP connection, camera activation, door command, arbitrary URL fetch,
outbound event callback, file access, credential logging or administration is
performed here. The manager must call set_motion only from its HA event loop,
after confirming actual muxed video; set_motion(False) on fallback or failure.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import hmac
import ipaddress
import math
import re
import secrets
import time
import uuid
import xml.etree.ElementTree as ET
from collections import deque
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from urllib.parse import urlsplit

from aiohttp import web
from defusedxml import ElementTree as SafeET
from defusedxml.common import DefusedXmlException

SOAP11 = "http://schemas.xmlsoap.org/soap/envelope/"
SOAP12 = "http://www.w3.org/2003/05/soap-envelope"
DEVICE = "http://www.onvif.org/ver10/device/wsdl"
MEDIA = "http://www.onvif.org/ver10/media/wsdl"
EVENTS = "http://www.onvif.org/ver10/events/wsdl"
SCHEMA = "http://www.onvif.org/ver10/schema"
TOPICS = "http://www.onvif.org/ver10/topics"
WSNT = "http://docs.oasis-open.org/wsn/b-2"
WSTOP = "http://docs.oasis-open.org/wsn/t-1"
WSA = "http://www.w3.org/2005/08/addressing"
WSSE = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd"
WSU = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd"
ERRORS = "http://www.onvif.org/ver10/error"
XS = "http://www.w3.org/2001/XMLSchema"
PASSWORD_DIGEST = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordDigest"
BASE64_BINARY = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-soap-message-security-1.0#Base64Binary"
CONCRETE = "http://docs.oasis-open.org/wsn/t-1/TopicExpression/Concrete"
CONCRETE_SET = "http://www.onvif.org/ver10/tev/topicExpression/ConcreteSet"
MOTION_TOPIC = "tns1:RuleEngine/CellMotionDetector/Motion"
MAX_BODY = 65536
MAX_HEADERS = 16384
MAX_REQUESTS = 32
MAX_PULL_POINTS = 16
MAX_MESSAGES = 128
MAX_PULL_LIMIT = 64
MAX_PULL_TIMEOUT = 30.0
MAX_LEASE = 300.0
MAX_SNAPSHOT = 2 * 1024 * 1024
FRESHNESS = 300.0
MAX_FUTURE = 30.0
CACHE_RETENTION = 331.0
_UTC = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?Z\Z")
_TOKEN = re.compile(r"[A-Za-z0-9_.-]{1,64}\Z")
_DURATION = re.compile(r"PT(?:(\d{1,4})H)?(?:(\d{1,4})M)?(?:(\d{1,4}(?:\.\d{1,3})?)S)?\Z")
_ENCODING = re.compile(r"\A\s*<\?xml\s+[^?]*?encoding\s*=\s*(['\"])([^'\"]+)\1", re.I)

for _prefix, _namespace in (("s", SOAP12), ("tds", DEVICE), ("trt", MEDIA),
                            ("tev", EVENTS), ("tt", SCHEMA), ("tns1", TOPICS),
                            ("wsnt", WSNT), ("wstop", WSTOP), ("wsa", WSA),
                            ("ter", ERRORS), ("xs", XS)):
    ET.register_namespace(_prefix, _namespace)


def q(namespace: str, name: str) -> str:
    return "{" + namespace + "}" + name


def add(parent: ET.Element, namespace: str, name: str, text: str | None = None,
        **attributes: str) -> ET.Element:
    element = ET.SubElement(parent, q(namespace, name), attributes)
    element.text = text
    return element


class Rejected(Exception):
    """Fixed errors only; never constructed from request or credential values."""

    def __init__(self, code: str = "InvalidArgVal", status: int = 400) -> None:
        if code not in {"InvalidArgVal", "ActionNotSupported", "NotAuthorized",
                        "ResourceUnknown", "MaxPullPoints", "MaxConcurrentPullRequests",
                        "ServiceUnavailable"}:
            raise ValueError("invalid_error_code")
        self.code, self.status = code, status
        super().__init__(code)


def _whitespace(element: ET.Element) -> None:
    if any(value and any(char not in " \t\r\n" for char in value)
           for value in (element.text, *(child.tail for child in element))):
        raise Rejected()


def parse_envelope(body: bytes) -> tuple[ET.Element, str, ET.Element]:
    """One bounded, entity/DTD/external-reference-disabled parse for dispatch."""
    if not isinstance(body, bytes) or not 0 < len(body) <= MAX_BODY:
        raise Rejected()
    try:
        text = body.decode("utf-8-sig", errors="strict")
        if any(ord(char) < 32 and char not in "\t\r\n" for char in text):
            raise ValueError()
        # Also exclude comments/CDATA/processing instructions from this subset.
        if "<!" in text or re.search(r"<\?(?!xml\s)", text, re.I):
            raise ValueError()
        encoding = _ENCODING.search(text)
        if encoding and encoding.group(2).lower() != "utf-8":
            raise ValueError()
        root = SafeET.fromstring(text, forbid_dtd=True, forbid_entities=True,
                                 forbid_external=True)
    except (ValueError, UnicodeError, ET.ParseError, DefusedXmlException):
        raise Rejected() from None
    nodes = []
    stack = [(root, 1)]
    while stack:
        element, depth = stack.pop()
        if depth > 32 or len(nodes) >= 1024:
            raise Rejected()
        nodes.append(element)
        stack.extend((child, depth + 1) for child in element)
    namespace = next((ns for ns in (SOAP11, SOAP12) if root.tag == q(ns, "Envelope")), None)
    if namespace is None or root.attrib:
        raise Rejected()
    children = list(root)
    if [element.tag for element in children] not in (
            [q(namespace, "Body")], [q(namespace, "Header"), q(namespace, "Body")]):
        raise Rejected()
    for element in [root, *children]:
        if element.attrib:
            raise Rejected()
        _whitespace(element)
    soap_body = children[-1]
    if len(soap_body) != 1:
        raise Rejected()
    # Refuse wrapping/lookalike structural elements anywhere inside the payload.
    if sum(node.tag.rsplit("}", 1)[-1] == "Body" for node in nodes) != 1:
        raise Rejected()
    if sum(node.tag.rsplit("}", 1)[-1] == "Header" for node in nodes) != len(children) - 1:
        raise Rejected()
    return root, namespace, soap_body[0]


def _leaf(element: ET.Element, limit: int, attributes: set[str] | None = None) -> str:
    if len(element) or set(element.attrib) - (attributes or set()):
        raise Rejected()
    if element.text is None or not 0 < len(element.text) <= limit:
        raise Rejected()
    return element.text


def _base64(value: str, minimum: int, maximum: int) -> bytes:
    clean = re.sub(r"[ \t\r\n]", "", value)
    try:
        encoded = clean.encode("ascii")
        decoded = base64.b64decode(encoded, validate=True)
    except (UnicodeError, ValueError, binascii.Error):
        raise Rejected("NotAuthorized", 401) from None
    if not minimum <= len(decoded) <= maximum or base64.b64encode(decoded) != encoded:
        raise Rejected("NotAuthorized", 401)
    return decoded


def _timestamp(value: str) -> float:
    if not _UTC.fullmatch(value):
        raise Rejected()
    try:
        return datetime.fromisoformat(value[:-1] + "+00:00").timestamp()
    except (ValueError, OverflowError, OSError):
        raise Rejected() from None


class UsernameTokenValidator:
    """Same audited digest/freshness/cache policy, validating the dispatch tree."""

    def __init__(self, username: str, password: str, *, limit: int = 4096,
                 clock: Callable[[], float] = time.time,
                 monotonic: Callable[[], float] = time.monotonic) -> None:
        self._user, self._password = username.encode("utf-8"), password.encode("utf-8")
        if not 1 <= limit <= 65536:
            raise ValueError("invalid_auth_configuration")
        self._limit, self._clock, self._monotonic = limit, clock, monotonic
        self._seen: dict[bytes, tuple[float, float]] = {}
        self._last_wall = self._last_mono = float("-inf")

    def validate(self, root: ET.Element, namespace: str) -> None:
        try:
            nodes = list(root.iter())
            securities = [e for e in nodes if e.tag.rsplit("}", 1)[-1] == "Security"]
            tokens = [e for e in nodes if e.tag.rsplit("}", 1)[-1] == "UsernameToken"]
            if len(securities) != 1 or len(tokens) != 1:
                raise Rejected()
            security, token = securities[0], tokens[0]
            header = root.find(q(namespace, "Header"))
            if header is None or security not in list(header) or security.tag != q(WSSE, "Security"):
                raise Rejected()
            if list(security) != [token] or token.tag != q(WSSE, "UsernameToken"):
                raise Rejected()
            _whitespace(security)
            _whitespace(token)
            allowed = {q(namespace, "mustUnderstand")}
            # Exactly the observed Protect SOAP12 spelling; not a general relaxation.
            if namespace == SOAP12 and security.attrib == {"mustUnderstand": "1"}:
                allowed = {"mustUnderstand"}
            if set(security.attrib) - allowed:
                raise Rejected()
            value = security.get(q(namespace, "mustUnderstand"))
            if value not in ({None, "0", "1"} if namespace == SOAP11 else {None, "0", "1", "false", "true"}):
                raise Rejected()
            if set(token.attrib) - {q(WSU, "Id")}:
                raise Rejected()
            token_id = token.get(q(WSU, "Id"))
            if token_id is not None and not 1 <= len(token_id) <= 256:
                raise Rejected()
            expected = {q(WSSE, "Username"), q(WSSE, "Password"), q(WSSE, "Nonce"), q(WSU, "Created")}
            if len(token) != 4 or {e.tag for e in token} != expected:
                raise Rejected()
            fields = {e.tag: e for e in token}
            username = _leaf(fields[q(WSSE, "Username")], 256).encode("utf-8")
            password_node = fields[q(WSSE, "Password")]
            supplied = _leaf(password_node, 128, {"Type"})
            if password_node.get("Type") != PASSWORD_DIGEST:
                raise Rejected()
            nonce_node = fields[q(WSSE, "Nonce")]
            nonce_text = _leaf(nonce_node, 1024, {"EncodingType"})
            if nonce_node.get("EncodingType") not in (None, BASE64_BINARY):
                raise Rejected()
            created = _leaf(fields[q(WSU, "Created")], 64)
            created_at = _timestamp(created)
            digest, nonce = _base64(supplied, 20, 20), _base64(nonce_text, 1, 128)
            expected_digest = hashlib.sha1(nonce + created.encode("utf-8") + self._password).digest()
            user_ok = hmac.compare_digest(username, self._user)
            digest_ok = hmac.compare_digest(digest, expected_digest)
            if not (user_ok & digest_ok):
                raise Rejected()
            # Entire method contains no await: freshness and cache insertion are
            # atomic on the HA event loop. Calls from other threads are forbidden.
            now, mono = float(self._clock()), float(self._monotonic())
            if not math.isfinite(now) or not math.isfinite(mono):
                raise Rejected()
            if now < self._last_wall or mono < self._last_mono:
                raise Rejected()
            self._last_wall, self._last_mono = now, mono
            if now - created_at > FRESHNESS or created_at - now > MAX_FUTURE:
                raise Rejected()
            expired = [key for key, (end_mono, end_wall) in self._seen.items()
                       if mono > end_mono and now > end_wall]
            for key in expired:
                del self._seen[key]
            key = hashlib.sha256(nonce).digest()
            if key in self._seen or len(self._seen) >= self._limit:
                raise Rejected()
            self._seen[key] = (mono + CACHE_RETENTION, now + CACHE_RETENTION)
        except Exception:
            raise Rejected("NotAuthorized", 401) from None


@dataclass(slots=True)
class _Notification:
    enabled: bool
    timestamp: str
    initialized: bool = False


@dataclass(slots=True)
class _Subscription:
    peer: str
    expires: float
    queue: deque[_Notification] = field(default_factory=deque)
    changed: asyncio.Event = field(default_factory=asyncio.Event)
    pulling: bool = False
    closed: bool = False


def _utc(now: float | None = None) -> str:
    return datetime.fromtimestamp(time.time() if now is None else now, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _duration(value: str, maximum: float, *, minimum: float = 0.0) -> float:
    match = _DURATION.fullmatch(value)
    if not match or not any(v is not None for v in match.groups()):
        raise Rejected()
    seconds = sum(float(v or 0) * multiplier for v, multiplier in zip(match.groups(), (3600, 60, 1)))
    if not minimum <= seconds <= maximum:
        raise Rejected()
    return seconds


class ONVIFServer:
    """Dedicated aiohttp listener; start/stop are awaited, no import side effects.

    All methods must run on the same HA event loop. allowed_peers is an exact
    nonempty set of IPv4 addresses; X-Forwarded-For is never trusted. Configuration
    and the real RTSP listener are owned/validated by the manager, not by requests.
    """

    def __init__(self, *, bind_host: str, port: int, advertised_host: str,
                 rtsp_uri: str, username: str, password: str, device_id: str,
                 allowed_peers: Iterable[str], profile_token: str = "standby",
                 encoder_token: str = "vec", serial_number: str | None = None,
                 mac_address: str | None = None, manufacturer: str = "Home Assistant",
                 model: str = "HA ONVIF Exporter", firmware_version: str = "0.1.0",
                 snapshot_provider: Callable[[], Awaitable[bytes]] | None = None,
                 clock: Callable[[], float] = time.time,
                 monotonic: Callable[[], float] = time.monotonic) -> None:
        try:
            self.bind_host = str(ipaddress.IPv4Address(bind_host))
            self.advertised_host = str(ipaddress.IPv4Address(advertised_host))
            self.allowed_peers = frozenset(str(ipaddress.IPv4Address(peer)) for peer in allowed_peers)
            if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
                raise ValueError()
            if not self.allowed_peers or len(self.allowed_peers) > 32:
                raise ValueError()
            if not _TOKEN.fullmatch(profile_token) or profile_token in {".", ".."}:
                raise ValueError()
            if not _TOKEN.fullmatch(encoder_token) or encoder_token in {".", ".."}:
                raise ValueError()
            if not isinstance(username, str) or not 1 <= len(username) <= 128 or ":" in username:
                raise ValueError()
            if not isinstance(password, str) or not 16 <= len(password) <= 512:
                raise ValueError()
            if any(ord(char) < 32 or ord(char) == 127 for char in username + password):
                raise ValueError()
            for value in (device_id, manufacturer, model, firmware_version, serial_number or device_id):
                if not isinstance(value, str) or not 1 <= len(value) <= 128 or any(ord(c) < 32 for c in value):
                    raise ValueError()
            uri = urlsplit(rtsp_uri)
            if (uri.scheme != "rtsp" or uri.hostname != self.advertised_host or
                    uri.username is not None or uri.password is not None or
                    uri.query or uri.fragment or uri.port is None or not 1 <= uri.port <= 65535 or
                    not re.fullmatch(r"/[A-Za-z0-9_.-]{1,128}", uri.path) or uri.path in {"/.", "/.."}):
                raise ValueError()
            identity = hashlib.sha256(device_id.encode("utf-8")).digest()
            derived_mac = bytes([2]) + identity[:5]
            selected_mac = mac_address or ":".join(f"{value:02x}" for value in derived_mac)
            if not re.fullmatch(r"(?:[0-9a-fA-F]{2}:){5}[0-9a-fA-F]{2}", selected_mac):
                raise ValueError()
        except (ValueError, TypeError, UnicodeError):
            raise ValueError("invalid_onvif_configuration") from None
        self.port, self.profile_token, self.encoder_token = port, profile_token, encoder_token
        self.rtsp_uri, self.serial_number, self.mac_address = rtsp_uri, serial_number or device_id, selected_mac.lower()
        self.device_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, "ha-onvif-exporter:" + device_id))
        self.manufacturer, self.model, self.firmware_version = manufacturer, model, firmware_version
        self._basic = (username + ":" + password).encode("utf-8")
        self._validator = UsernameTokenValidator(username, password, clock=clock, monotonic=monotonic)
        self._clock, self._monotonic, self._snapshot = clock, monotonic, snapshot_provider
        self._subscriptions: dict[str, _Subscription] = {}
        self._motion = False
        self._inflight = 0
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stopping = False
        self.app = web.Application(client_max_size=MAX_BODY,
                                   handler_args={"auto_decompress": False})
        for path in ("/onvif/", "/onvif/device_service", "/onvif/media_service", "/onvif/events_service"):
            self.app.router.add_post(path, self._handle)
        self.app.router.add_post("/onvif/subscription/{subscription:[a-f0-9]{32}}", self._handle)
        self.app.router.add_get("/onvif/snapshot/{profile}", self._snapshot_handle, allow_head=False)

    @property
    def base_url(self) -> str:
        return f"http://{self.advertised_host}:{self.port}"

    def _same_loop(self) -> None:
        if self._loop is not None and asyncio.get_running_loop() is not self._loop:
            raise RuntimeError("onvif_wrong_event_loop")

    async def start(self) -> None:
        if self._runner is not None:
            raise RuntimeError("onvif_already_started")
        self._loop, self._stopping = asyncio.get_running_loop(), False
        runner = web.AppRunner(self.app, access_log=None, shutdown_timeout=5,
                               max_line_size=8192, max_field_size=8192)
        try:
            await runner.setup()
            site = web.TCPSite(runner, self.bind_host, self.port, backlog=32)
            await site.start()
        except BaseException:
            await runner.cleanup()
            raise
        self._runner, self._site = runner, site

    async def stop(self) -> None:
        self._same_loop()
        self._stopping = True
        for subscription in self._subscriptions.values():
            subscription.closed = True
            subscription.changed.set()
        self._subscriptions.clear()
        runner, self._runner, self._site = self._runner, None, None
        if runner is not None:
            await runner.cleanup()

    def set_motion(self, enabled: bool) -> None:
        self._same_loop()
        if not isinstance(enabled, bool):
            raise ValueError("motion_boolean_required")
        if self._stopping or enabled == self._motion:
            return
        self._motion = enabled
        self._expire()
        notification = _Notification(enabled, _utc(self._clock()))
        for subscription in self._subscriptions.values():
            self._enqueue(subscription, notification)

    def _enqueue(self, subscription: _Subscription, notification: _Notification) -> None:
        if len(subscription.queue) >= MAX_MESSAGES:
            # Do not leave an overwhelmed subscriber stuck in a stale state.
            subscription.queue.clear()
            notification = _Notification(self._motion, notification.timestamp, True)
        subscription.queue.append(notification)
        subscription.changed.set()

    def _expire(self) -> None:
        now = self._monotonic()
        expired = [key for key, sub in self._subscriptions.items() if sub.expires <= now]
        for key in expired:
            sub = self._subscriptions.pop(key)
            sub.closed = True
            sub.changed.set()

    def _authorized_basic(self, value: str | None) -> bool:
        if value is None or not value.startswith("Basic ") or len(value) > 2048:
            return False
        try:
            encoded = value[6:].encode("ascii")
            decoded = base64.b64decode(encoded, validate=True)
            return base64.b64encode(decoded) == encoded and hmac.compare_digest(decoded, self._basic)
        except (UnicodeError, ValueError, binascii.Error):
            return False

    def _authenticate(self, root: ET.Element, namespace: str, operation: ET.Element,
                      authorization: str | None) -> None:
        # A malformed WS token cannot fall back to Basic/public time.
        has_security = any(e.tag.rsplit("}", 1)[-1] in {"Security", "UsernameToken"} for e in root.iter())
        if has_security:
            self._validator.validate(root, namespace)
            if authorization is not None and not self._authorized_basic(authorization):
                raise Rejected("NotAuthorized", 401)
        elif authorization is not None:
            if not self._authorized_basic(authorization):
                raise Rejected("NotAuthorized", 401)
        elif operation.tag != q(DEVICE, "GetSystemDateAndTime"):
            raise Rejected("NotAuthorized", 401)

    def _check_request(self, request: web.Request) -> str:
        peer = request.remote
        if peer not in self.allowed_peers:
            raise Rejected("NotAuthorized", 403)
        if self._stopping:
            raise Rejected("ServiceUnavailable", 503)
        if request.query_string or request.content_type not in {"application/soap+xml", "text/xml"}:
            raise Rejected()
        if request.headers.get("Content-Encoding", "identity") != "identity":
            raise Rejected()
        if sum(len(k) + len(v) + 4 for k, v in request.headers.items()) > MAX_HEADERS:
            raise Rejected()
        for name in ("Authorization", "Host", "Content-Length", "Content-Type", "SOAPAction"):
            if len(request.headers.getall(name, [])) > 1:
                raise Rejected()
        if request.content_length is not None and request.content_length > MAX_BODY:
            raise Rejected()
        return peer

    async def _handle(self, request: web.Request) -> web.Response:
        namespace = SOAP12
        counted = False
        try:
            peer = self._check_request(request)
            if self._inflight >= MAX_REQUESTS:
                raise Rejected("ServiceUnavailable", 503)
            self._inflight += 1
            counted = True
            data = bytearray()
            async with asyncio.timeout(3):
                async for chunk in request.content.iter_chunked(4096):
                    data.extend(chunk)
                    if len(data) > MAX_BODY:
                        raise Rejected()
            root, namespace, operation = parse_envelope(bytes(data))
            self._authenticate(root, namespace, operation, request.headers.get("Authorization"))
            addressing = self._addressing(root, namespace, operation)
            subscription_id = request.match_info.get("subscription")
            result = await self._dispatch(operation, peer, subscription_id)
            return self._response(namespace, result, addressing=addressing)
        except Rejected as error:
            response = self._fault(namespace, error.code, error.status)
            if error.status == 401:
                response.headers["WWW-Authenticate"] = 'Basic realm="HA ONVIF Exporter", charset="UTF-8"'
            return response
        except TimeoutError:
            return self._fault(namespace, "InvalidArgVal", 400)
        except (ConnectionError, OSError):
            return self._fault(namespace, "ServiceUnavailable", 503)
        except Exception:
            # A provider/clock failure must not place exception values in logs.
            return self._fault(namespace, "ServiceUnavailable", 503)
        finally:
            if counted:
                self._inflight -= 1

    def _action(self, operation: ET.Element, response: bool = False) -> str:
        namespace, name = operation.tag[1:].split("}", 1)
        if namespace == EVENTS:
            service = "PullPointSubscription" if name.startswith(("PullMessages", "SetSynchronizationPoint")) else "EventPortType"
            return EVENTS + "/" + service + "/" + name + ("" if response else "Request")
        if namespace == WSNT:
            return "http://docs.oasis-open.org/wsn/bw-2/SubscriptionManager/" + name + ("" if response else "Request")
        return namespace + "/" + name

    def _addressing(self, root: ET.Element, namespace: str, operation: ET.Element) -> str | None:
        header = root.find(q(namespace, "Header"))
        if header is None:
            return None
        actions = [e for e in header if e.tag == q(WSA, "Action")]
        message_ids = [e for e in header if e.tag == q(WSA, "MessageID")]
        if len(actions) > 1 or len(message_ids) > 1:
            raise Rejected()
        if actions and _leaf(actions[0], 256, {q(namespace, "mustUnderstand")}) != self._action(operation):
            raise Rejected()
        if not message_ids:
            return "" if actions else None
        value = _leaf(message_ids[0], 128)
        if not re.fullmatch(r"[A-Za-z0-9:/_.-]{1,128}", value):
            raise Rejected()
        # ReplyTo/To are never followed and no outbound SOAP request is made.
        return value

    def _response(self, namespace: str, response: ET.Element, status: int = 200,
                  addressing: str | None = None) -> web.Response:
        envelope = ET.Element(q(namespace, "Envelope"))
        # QName values in text/attributes require explicit declarations.
        if addressing is not None:
            header = add(envelope, namespace, "Header")
            add(header, WSA, "Action", self._action(response, True))
            if addressing:
                add(header, WSA, "RelatesTo", addressing)
        body = add(envelope, namespace, "Body")
        body.append(response)
        used_namespaces = {name.split("}", 1)[0][1:]
                           for element in envelope.iter()
                           for name in (element.tag, *element.attrib)
                           if name.startswith("{")}
        for prefix, uri in (("tns1", TOPICS), ("xs", XS), ("ter", ERRORS)):
            if uri not in used_namespaces:
                envelope.set("xmlns:" + prefix, uri)
        content_type = "text/xml" if namespace == SOAP11 else "application/soap+xml"
        return web.Response(body=ET.tostring(envelope, encoding="utf-8", xml_declaration=True),
                            status=status, content_type=content_type, charset="utf-8",
                            headers={"Cache-Control": "no-store"})

    def _fault(self, namespace: str, code: str, status: int) -> web.Response:
        fault = ET.Element(q(namespace, "Fault"))
        if namespace == SOAP12:
            node = add(fault, namespace, "Code")
            add(node, namespace, "Value", "s:Sender" if status < 500 else "s:Receiver")
            subcode = add(node, namespace, "Subcode")
            add(subcode, namespace, "Value", "ter:" + code)
            reason = add(fault, namespace, "Reason")
            add(reason, namespace, "Text", "Request rejected", **{q("http://www.w3.org/XML/1998/namespace", "lang"): "en"})
        else:
            ET.SubElement(fault, "faultcode").text = "ter:" + code
            ET.SubElement(fault, "faultstring").text = "Request rejected"
        return self._response(namespace, fault, status)

    def _fields(self, operation: ET.Element, allowed: dict[str, tuple[int, int]]) -> dict[str, ET.Element]:
        if operation.attrib:
            raise Rejected()
        _whitespace(operation)
        children = list(operation)
        if any(child.tag not in allowed for child in children):
            raise Rejected()
        for tag, (minimum, maximum) in allowed.items():
            if not minimum <= sum(child.tag == tag for child in children) <= maximum:
                raise Rejected()
        return {child.tag: child for child in children}

    def _media_fields(self, operation: ET.Element, token_name: str, token: str,
                      optional: bool = False) -> None:
        tag = q(MEDIA, token_name)
        fields = self._fields(operation, {tag: (0 if optional else 1, 1)})
        if tag in fields and _leaf(fields[tag], 64) != token:
            raise Rejected()

    async def _dispatch(self, operation: ET.Element, peer: str,
                        subscription_id: str | None) -> ET.Element:
        if subscription_id is not None:
            return await self._subscription_dispatch(operation, peer, subscription_id)
        if operation.tag.startswith("{" + DEVICE + "}"):
            return self._device(operation)
        if operation.tag.startswith("{" + MEDIA + "}"):
            return self._media(operation)
        if operation.tag == q(EVENTS, "GetServiceCapabilities"):
            self._fields(operation, {})
            return self._events_capabilities()
        if operation.tag == q(EVENTS, "GetEventProperties"):
            self._fields(operation, {})
            return self._event_properties()
        if operation.tag == q(EVENTS, "CreatePullPointSubscription"):
            return self._create_subscription(operation, peer)
        raise Rejected("ActionNotSupported", 400)

    def _device(self, operation: ET.Element) -> ET.Element:
        name = operation.tag.rsplit("}", 1)[-1]
        permitted = {"GetCapabilities", "GetServices", "GetServiceCapabilities", "GetDeviceInformation",
                     "GetSystemDateAndTime", "GetScopes", "GetDiscoveryMode", "GetNetworkInterfaces",
                     "GetNetworkProtocols", "GetHostname", "GetDNS", "GetNTP", "GetNetworkDefaultGateway"}
        if name not in permitted:
            raise Rejected("ActionNotSupported", 400)
        allowed = {q(DEVICE, "IncludeCapability"): (0, 1)} if name == "GetServices" else (
            {q(DEVICE, "Category"): (0, 1)} if name == "GetCapabilities" else {})
        fields = self._fields(operation, allowed)
        response = ET.Element(q(DEVICE, name + "Response"))
        if name == "GetDeviceInformation":
            for key, value in (("Manufacturer", self.manufacturer), ("Model", self.model),
                               ("FirmwareVersion", self.firmware_version), ("SerialNumber", self.serial_number),
                               ("HardwareId", self.device_uuid)):
                add(response, DEVICE, key, value)
        elif name == "GetSystemDateAndTime":
            info = add(response, DEVICE, "SystemDateAndTime")
            add(info, SCHEMA, "DateTimeType", "Manual")
            add(info, SCHEMA, "DaylightSavings", "false")
            add(add(info, SCHEMA, "TimeZone"), SCHEMA, "TZ", "UTC0")
            date = datetime.fromtimestamp(self._clock(), timezone.utc)
            stamp = add(info, SCHEMA, "UTCDateTime")
            daytime, calendar = add(stamp, SCHEMA, "Time"), add(stamp, SCHEMA, "Date")
            for key, value in (("Hour", date.hour), ("Minute", date.minute), ("Second", date.second)):
                add(daytime, SCHEMA, key, str(value))
            for key, value in (("Year", date.year), ("Month", date.month), ("Day", date.day)):
                add(calendar, SCHEMA, key, str(value))
        elif name == "GetServices":
            include = fields.get(q(DEVICE, "IncludeCapability"))
            include_value = _leaf(include, 5) if include is not None else "false"
            if include_value not in {"true", "false", "1", "0"}:
                raise Rejected()
            for ns, path in ((DEVICE, "device"), (MEDIA, "media"), (EVENTS, "events")):
                service = add(response, DEVICE, "Service")
                add(service, DEVICE, "Namespace", ns)
                add(service, DEVICE, "XAddr", self.base_url + f"/onvif/{path}_service")
                if include_value in {"true", "1"}:
                    wrapper = add(service, DEVICE, "Capabilities")
                    if ns == MEDIA:
                        wrapper.append(self._media_capabilities())
                    elif ns == EVENTS:
                        wrapper.append(self._events_capabilities()[0])
                    else:
                        wrapper.append(self._device_capabilities())
                version = add(service, DEVICE, "Version")
                add(version, SCHEMA, "Major", "2")
                add(version, SCHEMA, "Minor", "5")
        elif name == "GetCapabilities":
            category = fields.get(q(DEVICE, "Category"))
            requested = _leaf(category, 32) if category is not None else "All"
            if requested not in {"All", "Device", "Media", "Events"}:
                raise Rejected("ActionNotSupported", 400)
            caps = add(response, DEVICE, "Capabilities")
            for service_name, path in (("Device", "device"), ("Media", "media"), ("Events", "events")):
                if requested not in {"All", service_name}:
                    continue
                service = add(caps, SCHEMA, service_name)
                add(service, SCHEMA, "XAddr", self.base_url + f"/onvif/{path}_service")
                if service_name == "Media":
                    streaming = add(service, SCHEMA, "StreamingCapabilities")
                    for key, value in (("RTPMulticast", "false"), ("RTP_TCP", "false"), ("RTP_RTSP_TCP", "true")):
                        add(streaming, SCHEMA, key, value)
                elif service_name == "Events":
                    for key, value in (("WSSubscriptionPolicySupport", "false"), ("WSPullPointSupport", "true"),
                                       ("WSPausableSubscriptionManagerInterfaceSupport", "false")):
                        add(service, SCHEMA, key, value)
        elif name == "GetServiceCapabilities":
            response.append(self._device_capabilities())
        elif name == "GetScopes":
            # No Profile S/T certification scope is advertised.
            for scope in ("onvif://www.onvif.org/name/HA-ONVIF-Exporter",
                          "onvif://www.onvif.org/type/Network_Video_Transmitter"):
                item = add(response, DEVICE, "Scopes")
                add(item, SCHEMA, "ScopeDef", "Fixed")
                add(item, SCHEMA, "ScopeItem", scope)
        elif name == "GetDiscoveryMode":
            add(response, DEVICE, "DiscoveryMode", "NonDiscoverable")
        elif name == "GetNetworkInterfaces":
            interface = add(response, DEVICE, "NetworkInterfaces", token="virtual")
            add(interface, SCHEMA, "Enabled", "true")
            info = add(interface, SCHEMA, "Info")
            add(info, SCHEMA, "Name", "virtual")
            add(info, SCHEMA, "HwAddress", self.mac_address)
        elif name == "GetNetworkProtocols":
            for protocol, port in (("HTTP", self.port), ("RTSP", urlsplit(self.rtsp_uri).port)):
                item = add(response, DEVICE, "NetworkProtocols")
                add(item, SCHEMA, "Name", protocol)
                add(item, SCHEMA, "Enabled", "true")
                add(item, SCHEMA, "Port", str(port))
        elif name == "GetHostname":
            info = add(response, DEVICE, "HostnameInformation")
            add(info, SCHEMA, "FromDHCP", "false")
            add(info, SCHEMA, "Name", "ha-onvif-exporter")
        elif name in {"GetDNS", "GetNTP"}:
            info = add(response, DEVICE, "DNSInformation" if name == "GetDNS" else "NTPInformation")
            add(info, SCHEMA, "FromDHCP", "false")
        elif name == "GetNetworkDefaultGateway":
            add(response, DEVICE, "NetworkGateway")
        return response

    def _device_capabilities(self) -> ET.Element:
        caps = ET.Element(q(DEVICE, "Capabilities"))
        add(caps, DEVICE, "Network", IPFilter="false", ZeroConfiguration="false", IPVersion6="false", DynDNS="false")
        add(caps, DEVICE, "Security", TLS1_0="false", TLS1_1="false", TLS1_2="false",
            X_509Token="false", SAMLToken="false", KerberosToken="false", RELToken="false",
            UsernameToken="true", HttpDigest="false", RemoteUserHandling="false")
        add(caps, DEVICE, "System", DiscoveryResolve="false", DiscoveryBye="false",
            RemoteDiscovery="false", SystemBackup="false", SystemLogging="false", FirmwareUpgrade="false")
        return caps

    def _profile(self, parent: ET.Element, name: str) -> None:
        profile = add(parent, MEDIA, name, token=self.profile_token, fixed="true")
        add(profile, SCHEMA, "Name", self.profile_token)
        self._source_configuration(profile, SCHEMA, "VideoSourceConfiguration")
        self._encoder_configuration(profile, SCHEMA, "VideoEncoderConfiguration")

    def _source_configuration(self, parent: ET.Element, namespace: str, name: str) -> None:
        config = add(parent, namespace, name, token=self.profile_token)
        add(config, SCHEMA, "Name", "VSC")
        add(config, SCHEMA, "UseCount", "1")
        add(config, SCHEMA, "SourceToken", self.profile_token)
        add(config, SCHEMA, "Bounds", x="0", y="0", width="1920", height="1080")

    def _encoder_configuration(self, parent: ET.Element, namespace: str, name: str) -> None:
        config = add(parent, namespace, name, token=self.encoder_token)
        for key, value in (("Name", "VEC"), ("UseCount", "1"), ("Encoding", "H264")):
            add(config, SCHEMA, key, value)
        resolution = add(config, SCHEMA, "Resolution")
        add(resolution, SCHEMA, "Width", "1920")
        add(resolution, SCHEMA, "Height", "1080")
        add(config, SCHEMA, "Quality", "0")
        control = add(config, SCHEMA, "RateControl")
        for key, value in (("FrameRateLimit", "30"), ("EncodingInterval", "1"), ("BitrateLimit", "8192")):
            add(control, SCHEMA, key, value)
        codec = add(config, SCHEMA, "H264")
        add(codec, SCHEMA, "GovLength", "10")
        add(codec, SCHEMA, "H264Profile", "Main")
        multicast = add(config, SCHEMA, "Multicast")
        address = add(multicast, SCHEMA, "Address")
        add(address, SCHEMA, "Type", "IPv4")
        add(address, SCHEMA, "IPv4Address", "0.0.0.0")
        for key, value in (("Port", "0"), ("TTL", "0"), ("AutoStart", "false")):
            add(multicast, SCHEMA, key, value)
        add(config, SCHEMA, "SessionTimeout", "PT10S")

    def _media_capabilities(self) -> ET.Element:
        caps = ET.Element(q(MEDIA, "Capabilities"), {"SnapshotUri": str(self._snapshot is not None).lower(),
                          "Rotation": "false", "VideoSourceMode": "false", "OSD": "false"})
        add(caps, MEDIA, "ProfileCapabilities", MaximumNumberOfProfiles="1")
        add(caps, MEDIA, "StreamingCapabilities", RTPMulticast="false", RTP_TCP="false", RTP_RTSP_TCP="true")
        return caps

    def _media(self, operation: ET.Element) -> ET.Element:
        name = operation.tag.rsplit("}", 1)[-1]
        permitted = {"GetProfiles", "GetProfile", "GetVideoSources", "GetVideoSourceConfigurations",
                     "GetVideoSourceConfiguration", "GetVideoEncoderConfigurations", "GetVideoEncoderConfiguration",
                     "GetVideoEncoderConfigurationOptions", "GetStreamUri", "GetSnapshotUri", "GetServiceCapabilities",
                     "GetAudioSources", "GetAudioSourceConfigurations", "GetAudioEncoderConfigurations"}
        if name not in permitted:
            raise Rejected("ActionNotSupported", 400)
        if name in {"GetProfile", "GetSnapshotUri"}:
            self._media_fields(operation, "ProfileToken", self.profile_token)
        elif name in {"GetVideoEncoderConfiguration", "GetVideoSourceConfiguration"}:
            self._media_fields(operation, "ConfigurationToken", self.encoder_token if name == "GetVideoEncoderConfiguration" else self.profile_token)
        elif name == "GetVideoEncoderConfigurationOptions":
            fields = self._fields(operation, {q(MEDIA, "ConfigurationToken"): (0, 1), q(MEDIA, "ProfileToken"): (0, 1)})
            for tag, value in ((q(MEDIA, "ConfigurationToken"), self.encoder_token), (q(MEDIA, "ProfileToken"), self.profile_token)):
                if tag in fields and _leaf(fields[tag], 64) != value:
                    raise Rejected()
        elif name == "GetStreamUri":
            fields = self._fields(operation, {q(MEDIA, "StreamSetup"): (1, 1), q(MEDIA, "ProfileToken"): (1, 1)})
            if _leaf(fields[q(MEDIA, "ProfileToken")], 64) != self.profile_token:
                raise Rejected()
            setup = self._fields(fields[q(MEDIA, "StreamSetup")], {q(SCHEMA, "Stream"): (1, 1), q(SCHEMA, "Transport"): (1, 1)})
            if _leaf(setup[q(SCHEMA, "Stream")], 32) != "RTP-Unicast":
                raise Rejected()
            transport = self._fields(setup[q(SCHEMA, "Transport")], {q(SCHEMA, "Protocol"): (1, 1)})
            if _leaf(transport[q(SCHEMA, "Protocol")], 32) not in {"RTSP", "TCP"}:
                raise Rejected()
        else:
            self._fields(operation, {})
        response = ET.Element(q(MEDIA, name + "Response"))
        if name in {"GetProfiles", "GetProfile"}:
            self._profile(response, "Profiles" if name == "GetProfiles" else "Profile")
        elif name == "GetVideoSources":
            source = add(response, MEDIA, "VideoSources", token=self.profile_token)
            add(source, SCHEMA, "Framerate", "30")
            resolution = add(source, SCHEMA, "Resolution")
            add(resolution, SCHEMA, "Width", "1920")
            add(resolution, SCHEMA, "Height", "1080")
        elif name.startswith("GetVideoSourceConfiguration"):
            self._source_configuration(response, MEDIA, "Configurations" if name.endswith("s") else "Configuration")
        elif name in {"GetVideoEncoderConfigurations", "GetVideoEncoderConfiguration"}:
            self._encoder_configuration(response, MEDIA, "Configurations" if name.endswith("s") else "Configuration")
        elif name == "GetVideoEncoderConfigurationOptions":
            options = add(response, MEDIA, "Options")
            quality = add(options, SCHEMA, "QualityRange")
            add(quality, SCHEMA, "Min", "0")
            add(quality, SCHEMA, "Max", "0")
            codec = add(options, SCHEMA, "H264")
            resolution = add(codec, SCHEMA, "ResolutionsAvailable")
            add(resolution, SCHEMA, "Width", "1920")
            add(resolution, SCHEMA, "Height", "1080")
            for range_name, value in (("GovLengthRange", "10"), ("FrameRateRange", "30"), ("EncodingIntervalRange", "1")):
                interval = add(codec, SCHEMA, range_name)
                add(interval, SCHEMA, "Min", value)
                add(interval, SCHEMA, "Max", value)
            add(codec, SCHEMA, "H264ProfilesSupported", "Main")
        elif name in {"GetStreamUri", "GetSnapshotUri"}:
            if name == "GetSnapshotUri" and self._snapshot is None:
                raise Rejected("ActionNotSupported", 400)
            uri = self.rtsp_uri if name == "GetStreamUri" else self.base_url + "/onvif/snapshot/" + self.profile_token
            media_uri = add(response, MEDIA, "MediaUri")
            for key, value in (("Uri", uri), ("InvalidAfterConnect", "false"), ("InvalidAfterReboot", "false"), ("Timeout", "PT0S")):
                add(media_uri, SCHEMA, key, value)
        elif name == "GetServiceCapabilities":
            response.append(self._media_capabilities())
        # Empty Audio* responses truthfully advertise no exported audio.
        return response

    def _events_capabilities(self) -> ET.Element:
        response = ET.Element(q(EVENTS, "GetServiceCapabilitiesResponse"))
        add(response, EVENTS, "Capabilities", WSSubscriptionPolicySupport="false",
            WSPausableSubscriptionManagerInterfaceSupport="false", MaxNotificationProducers="0",
            MaxPullPoints=str(MAX_PULL_POINTS), PersistentNotificationStorage="false")
        return response

    def _event_properties(self) -> ET.Element:
        response = ET.Element(q(EVENTS, "GetEventPropertiesResponse"))
        add(response, EVENTS, "TopicNamespaceLocation", "http://www.onvif.org/onvif/ver10/topics/topicns.xml")
        add(response, WSNT, "FixedTopicSet", "true")
        topics = add(response, WSTOP, "TopicSet")
        engine = add(topics, TOPICS, "RuleEngine")
        detector = add(engine, TOPICS, "CellMotionDetector")
        motion = add(detector, TOPICS, "Motion", **{q(WSTOP, "topic"): "true"})
        description = add(motion, SCHEMA, "MessageDescription", IsProperty="true")
        source = add(description, SCHEMA, "Source")
        add(source, SCHEMA, "SimpleItemDescription", Name="VideoSourceConfigurationToken", Type="tt:ReferenceToken")
        add(source, SCHEMA, "SimpleItemDescription", Name="Rule", Type="xs:string")
        data = add(description, SCHEMA, "Data")
        add(data, SCHEMA, "SimpleItemDescription", Name="IsMotion", Type="xs:boolean")
        for dialect in (CONCRETE, CONCRETE_SET):
            add(response, WSNT, "TopicExpressionDialect", dialect)
        add(response, EVENTS, "MessageContentFilterDialect", "")
        add(response, EVENTS, "MessageContentSchemaLocation", "http://www.onvif.org/ver10/schema/onvif.xsd")
        return response

    def _lease(self, field: ET.Element | None) -> float:
        if field is None:
            return 60.0
        value = _leaf(field, 64)
        if value.startswith("PT"):
            return _duration(value, MAX_LEASE, minimum=1.0)
        duration = _timestamp(value) - self._clock()
        if not 1 <= duration <= MAX_LEASE:
            raise Rejected()
        return duration

    def _create_subscription(self, operation: ET.Element, peer: str) -> ET.Element:
        fields = self._fields(operation, {q(EVENTS, "Filter"): (0, 1), q(EVENTS, "InitialTerminationTime"): (0, 1)})
        if q(EVENTS, "Filter") in fields:
            filters = self._fields(fields[q(EVENTS, "Filter")], {q(WSNT, "TopicExpression"): (1, 1)})
            topic = filters[q(WSNT, "TopicExpression")]
            expression = _leaf(topic, 128, {"Dialect"})
            if topic.get("Dialect") not in {CONCRETE, CONCRETE_SET} or expression not in {MOTION_TOPIC, "tns1:RuleEngine//."}:
                raise Rejected("ActionNotSupported", 400)
        lease = self._lease(fields.get(q(EVENTS, "InitialTerminationTime")))
        self._expire()
        if len(self._subscriptions) >= MAX_PULL_POINTS:
            raise Rejected("MaxPullPoints", 400)
        key = secrets.token_hex(16)
        sub = _Subscription(peer, self._monotonic() + lease)
        self._subscriptions[key] = sub
        self._enqueue(sub, _Notification(self._motion, _utc(self._clock()), True))
        response = ET.Element(q(EVENTS, "CreatePullPointSubscriptionResponse"))
        endpoint = add(response, EVENTS, "SubscriptionReference")
        add(endpoint, WSA, "Address", self.base_url + "/onvif/subscription/" + key)
        add(response, WSNT, "CurrentTime", _utc(self._clock()))
        add(response, WSNT, "TerminationTime", _utc(self._clock() + lease))
        return response

    async def _subscription_dispatch(self, operation: ET.Element, peer: str, key: str) -> ET.Element:
        self._expire()
        sub = self._subscriptions.get(key)
        if sub is None or sub.peer != peer or sub.closed:
            raise Rejected("ResourceUnknown", 400)
        if operation.tag == q(EVENTS, "PullMessages"):
            fields = self._fields(operation, {q(EVENTS, "Timeout"): (1, 1), q(EVENTS, "MessageLimit"): (1, 1)})
            timeout = _duration(_leaf(fields[q(EVENTS, "Timeout")], 64), MAX_PULL_TIMEOUT)
            limit_text = _leaf(fields[q(EVENTS, "MessageLimit")], 3)
            if not re.fullmatch(r"[0-9]{1,3}", limit_text) or not 1 <= int(limit_text) <= MAX_PULL_LIMIT:
                raise Rejected()
            if sub.pulling:
                raise Rejected("MaxConcurrentPullRequests", 400)
            sub.pulling = True
            try:
                if not sub.queue and timeout:
                    sub.changed.clear()
                    try:
                        await asyncio.wait_for(sub.changed.wait(), min(timeout, max(0.0, sub.expires - self._monotonic())))
                    except TimeoutError:
                        pass
                if sub.closed or sub.expires <= self._monotonic():
                    raise Rejected("ResourceUnknown", 400)
                response = ET.Element(q(EVENTS, "PullMessagesResponse"))
                add(response, EVENTS, "CurrentTime", _utc(self._clock()))
                add(response, EVENTS, "TerminationTime", _utc(self._clock() + max(0.0, sub.expires - self._monotonic())))
                for _ in range(min(int(limit_text), len(sub.queue))):
                    self._notification_xml(response, sub.queue.popleft())
                return response
            finally:
                sub.pulling = False
        if operation.tag == q(EVENTS, "SetSynchronizationPoint"):
            self._fields(operation, {})
            self._enqueue(sub, _Notification(self._motion, _utc(self._clock()), True))
            return ET.Element(q(EVENTS, "SetSynchronizationPointResponse"))
        if operation.tag == q(WSNT, "Renew"):
            fields = self._fields(operation, {q(WSNT, "TerminationTime"): (0, 1)})
            lease = self._lease(fields.get(q(WSNT, "TerminationTime")))
            sub.expires = self._monotonic() + lease
            response = ET.Element(q(WSNT, "RenewResponse"))
            add(response, WSNT, "TerminationTime", _utc(self._clock() + lease))
            add(response, WSNT, "CurrentTime", _utc(self._clock()))
            return response
        if operation.tag == q(WSNT, "Unsubscribe"):
            self._fields(operation, {})
            sub.closed = True
            sub.changed.set()
            del self._subscriptions[key]
            return ET.Element(q(WSNT, "UnsubscribeResponse"))
        raise Rejected("ActionNotSupported", 400)

    def _notification_xml(self, parent: ET.Element, notification: _Notification) -> None:
        item = add(parent, WSNT, "NotificationMessage")
        add(item, WSNT, "Topic", MOTION_TOPIC, Dialect=CONCRETE)
        wrapper = add(item, WSNT, "Message")
        message = add(wrapper, SCHEMA, "Message", UtcTime=notification.timestamp,
                      PropertyOperation="Initialized" if notification.initialized else "Changed")
        source = add(message, SCHEMA, "Source")
        add(source, SCHEMA, "SimpleItem", Name="VideoSourceConfigurationToken", Value=self.profile_token)
        add(source, SCHEMA, "SimpleItem", Name="Rule", Value="VideoReady")
        data = add(message, SCHEMA, "Data")
        add(data, SCHEMA, "SimpleItem", Name="IsMotion", Value=str(notification.enabled).lower())

    async def _snapshot_handle(self, request: web.Request) -> web.Response:
        # GET has no SOAP UsernameToken. Same credentials via HTTP Basic, no
        # credentials in URLs; this callback may only return an existing JPEG.
        counted = False
        try:
            if request.remote not in self.allowed_peers:
                raise Rejected("NotAuthorized", 403)
            if sum(len(k) + len(v) + 4 for k, v in request.headers.items()) > MAX_HEADERS:
                raise Rejected()
            if request.query_string or request.match_info["profile"] != self.profile_token or self._snapshot is None:
                raise Rejected("ActionNotSupported", 404)
            if len(request.headers.getall("Authorization", [])) != 1 or not self._authorized_basic(request.headers.get("Authorization")):
                raise Rejected("NotAuthorized", 401)
            if self._stopping or self._inflight >= MAX_REQUESTS:
                raise Rejected("ServiceUnavailable", 503)
            self._inflight += 1
            counted = True
            async with asyncio.timeout(2):
                image = await self._snapshot()
            if not isinstance(image, bytes) or not 4 <= len(image) <= MAX_SNAPSHOT or not image.startswith(b"\xff\xd8") or not image.endswith(b"\xff\xd9"):
                raise Rejected("ServiceUnavailable", 503)
            return web.Response(body=image, content_type="image/jpeg", headers={"Cache-Control": "no-store"})
        except Rejected as error:
            response = web.Response(status=error.status, text="Request rejected")
            if error.status == 401:
                response.headers["WWW-Authenticate"] = 'Basic realm="HA ONVIF Exporter", charset="UTF-8"'
            return response
        except (TimeoutError, OSError):
            return web.Response(status=503, text="Request rejected")
        except Exception:
            return web.Response(status=503, text="Request rejected")
        finally:
            if counted:
                self._inflight -= 1
