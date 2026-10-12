# ONVIF exporter server: implemented local subset

`onvif_server.py` is a dedicated aiohttp listener loaded by the integration's
manager. It is not an ONVIF-certified device and has no independent daemon,
camera connection or camera/door control. No production deployment or Protect
adoption has been tested by this component's author.

## Manager contract

Instantiate `ONVIFServer` with `bind_host`, `port`, `advertised_host`,
`rtsp_uri`, `username`, `password`, stable `device_id`, and nonempty exact IPv4
`allowed_peers`. `profile_token="standby"` and `encoder_token="vec"` retain the
tokens used by the previous go2rtc exporter. Optional `serial_number` and
`mac_address` can preserve a separately verified previous virtual identity;
otherwise UUID/MAC derive deterministically from `device_id`. Stability of these
values is not proof that Protect preserves adoption or AI pairing.

The URI must be a credential-free RTSP URI on `advertised_host` with a positive
port and a single safe path segment. The caller owns and verifies that listener;
this module only returns its URI and never connects to it. Media metadata is the
manager's fixed H264 Main 1920×1080, 30 fps, GOP 10 contract, not a measurement.
Changing it requires a coordinated media-engine change and real client tests.

Call `await start()`/`await stop()` and synchronous `set_motion(bool)` on the
same HA event loop. Do not call them from an executor thread. Send true only
after actual successfully muxed live frames, false on standby/error/termination.
Triggers and the rolling video window are the manager's responsibility. Merely
requesting `GetStreamUri` does not activate capture or create motion.

Optional `snapshot_provider` is an async callback returning an already available
JPEG (not activating or contacting a camera). Without it SnapshotUri capability
is false and GetSnapshotUri returns ActionNotSupported. With it the returned
URI is this listener's own authenticated endpoint; callback output and duration
are bounded. It must not be an arbitrary URL downloader.

## Supported interface

- Device: read-only services/capabilities, virtual identity, UTC clock, scopes,
  discovery mode and basic virtual network metadata. No configuration setters,
  user management, reboot, firmware, logs or backups.
- Media1: one fixed profile, source/encoder configuration and options,
  credential-free GetStreamUri, optional GetSnapshotUri, and truthful empty
  audio lists. No audio export, PTZ, recording service or media mutation.
- Events: GetEventProperties/GetServiceCapabilities, CreatePullPointSubscription,
  PullMessages, SetSynchronizationPoint, Renew and Unsubscribe. No outbound
  Subscribe/Notify callbacks, event broker or stored event history.
- SOAP 1.1/1.2 and optional WS-Addressing Action/MessageID correlation; dispatch
  uses the authenticated parsed Body, not SOAPAction/regular expressions.

Motion topic is `tns1:RuleEngine/CellMotionDetector/Motion` with `IsMotion`.
New subscribers get Initialized current state, subsequent changes get Changed.
It is a video-ready recording hint, not pixel motion detection or recognition.
The manager must validate actual Protect interpretation before claiming recording
behavior; PullPoint unit tests are not a Protect/AI hardware acceptance test.

TopicSet uses a namespace-qualified root and unqualified descendant topic names.
Topic-expression filtering remains a limited implementation: it compares the
supported literal expressions rather than resolving root prefixes against their
in-scope namespace bindings. An equivalent expression with a different prefix
can therefore be rejected, and the supported literal prefix does not validate
its namespace binding. General QName filter interoperability is not claimed.
These verified protocol limitations do not establish the cause of any Protect
recording or subscription behavior.

## Security and bounds

Exact peer ACL uses the TCP peer, never forwarding headers. WS UsernameToken
PasswordDigest and canonical HTTP Basic are supported; HTTP Digest is not
advertised or implemented. Only the UTC GetSystemDateAndTime query is public,
and only to allowed peers; malformed supplied authentication still fails.
Protect's observed SOAP12 unqualified `mustUnderstand="1"` is accepted exactly.
Device-service security capabilities use the schema's exact attribute names
`TLS1.0`, `TLS1.1`, `TLS1.2` and `X.509Token`, not underscore aliases.

WS freshness is 300 seconds, future skew 30, replay retention 331, cache capacity
4096; valid credentials are compared in constant time. Cache is event-loop
atomic and exists only for this process lifetime. Clock regression/full cache
fail closed. PasswordDigest does not sign the Body. Basic/plain RTSP need a
trusted network or suitable secure transport; no TLS claim is made here.

XML is UTF-8, max 64 KiB, 1024 nodes/depth 32, parsed once with defusedxml
DTD/entities/external references disabled. HTTP decompression is disabled; query
parameters, compressed bodies, duplicate critical headers, wrapping and
unapproved operations/fields are rejected. No request bodies/credentials or raw
exceptions are logged. Access logging is disabled.

There are at most 32 active requests, 16 PullPoints, 128 queued notifications per
PullPoint, 64 messages per response, 60 seconds per pull, 300 seconds per lease,
3 seconds per request-body read, 2 seconds/2 MiB per snapshot. Queue overflow
coalesces to Initialized current state instead of retaining a stale motion flag.
Expired/unsubscribed/stopped subscriptions wake waiters. Pending cancellation
releases the per-subscription pull guard. No push URL is ever followed.

PullMessages accepts positive `xs:int` MessageLimit values through 2147483647,
including an optional plus sign and surrounding XML whitespace, but retrieves
at most 64 queued notifications per response. A supported timeout above 60 s
returns a typed PullMessages fault with MaxTimeout/MaxMessageLimit and its
WS-Addressing fault action, without waiting for that duration. The 60 s pull
bound does not change the manager's rolling 30 s live-video window, its triggers,
standby fallback or the existing subscription-lease behavior.

Subscription leases still expire at their existing termination time unless an
explicit Renew extends them; PullMessages does not automatically extend a lease.
ONVIF Core section 9.1.1 recommends keeping a subscription active for at least
the requested Timeout after the response. That keepalive recommendation is not
implemented here, and this compatibility limitation has not been established
as the cause of any particular client behavior.

## Local tests and sources

Run `test_onvif_server.py` with the isolated `.test-venv` Python. It uses public
fixture credentials and real aiohttp listeners only on 127.0.0.1. Dependencies
installed only into that workspace venv: aiohttp 3.14.3 (the HA 2026.8.1 core
pin) and defusedxml 0.7.1, plus aiohttp's dependencies. No HA installation is
modified by these tests.

Primary API references consulted (no third-party implementation source copied):

- [ONVIF Device WSDL](https://www.onvif.org/ver10/device/wsdl/devicemgmt.wsdl)
- [ONVIF Media WSDL](https://www.onvif.org/ver10/media/wsdl/media.wsdl)
- [ONVIF Events WSDL](https://www.onvif.org/ver10/events/wsdl/event.wsdl)
- [ONVIF Core Specification](https://www.onvif.org/specs/core/ONVIF-Core-Specification.pdf)
- [OASIS WS-Topics 1.3](https://docs.oasis-open.org/wsn/wsn-ws_topics-1.3-spec-os.htm)
- [OASIS UsernameToken Profile 1.1.1](https://docs.oasis-open.org/wss-m/wss/v1.1.1/os/wss-UsernameTokenProfile-v1.1.1-os.html)
- [defusedxml documentation](https://pypi.org/project/defusedxml/)
- [HA 2026.8.1 core requirements](https://github.com/home-assistant/core/blob/2026.8.1/requirements.txt)

The UsernameToken validator is original application-authored project code, not
a vendored third-party implementation requiring a copied-source license.
