# HA ONVIF Exporter

Experimental, video-only Home Assistant custom integration that exposes a
compatible HA camera as an authenticated virtual ONVIF/RTSP camera.

This is **not** an official Home Assistant, ONVIF, BTicino or UniFi integration,
and is not ONVIF-certified. Initial target: Home Assistant Container **2026.8.1
on Linux amd64**. Real recorder compatibility must be tested; passing local
tests does not certify Protect adoption, AI pairing or recording behavior.

## What it does

- Uses an existing Home Assistant camera and a receive-only HA WebRTC session.
- Exports a stable video-only H264 Main 1920×1080/30 RTSP profile.
- Shows a black **STANDBY** frame when no export window is active.
- Supports manual activation, filtered HA events, or an off-to-on binary sensor.
- Extends the configured window from the **latest accepted activation**; the
  default is 30 seconds. Video frames and recorder requests do not extend it.
- Emits ONVIF PullPoint `IsMotion` state only after confirmed live output, and
  clears it when capture ends. This is a recording hint, not image analysis.
- Offers UI configuration/options, separate recorder credentials and an exact
  client IPv4 allowlist.

It does not implement door unlocks, access-control decisions, facial recognition,
audio export, microphone/talkback, PTZ, or the recorder's recording policy.
Existing camera controls remain outside its scope. No camera firmware or
camera-integration source changes are performed by this integration.

## Compatibility corrections in 0.1.7

PullPoint accepts waits of up to 60 seconds and valid positive `xs:int`
message limits, while still returning at most 64 queued messages per response.
A timeout above the supported maximum returns the typed PullMessages fault
with both supported limits. Capability responses use schema order, accept
multiple supported categories, and use the standard security attribute names.
The motion TopicSet uses namespace-qualified roots and unqualified children.

These are protocol corrections, not a change to camera startup, the activation
window, credentials, peer restrictions, virtual identity or door automations.
They do not establish that any particular recorder subscribes to motion events;
that behavior still requires an actual recorder test.

## Requirements and limits

1. Linux x86_64/amd64 and the target HA camera/WebRTC APIs. ARM, Windows, macOS
   and other HA versions/deployment types are not validated by this release.
2. HA's managed **go2rtc** must already provide the selected camera's local
   RTSP alias. A camera entity or WebRTC capability alone is not enough.
   Native WebRTC-only cameras, camera-specific data-channel protocols and
   arbitrary external go2rtc configurations are not supported by this version.
3. The selected camera must produce usable video through that existing HA
   route. Validate the HA live view first; incompatible sources fail closed.
4. The host must permit this integration to run its owned media subprocesses,
   use a writable HA storage directory and bind the configured local ports.
   MediaMTX 1.21.2's pinned Linux amd64 release is included; Python media
   dependencies remain governed by HA and the integration manifest.
5. The recorder must reach the configured HA IPv4 address and both TCP ports.
   Allow only the recorder/client hosts actually needed. Do not expose these
   listeners to the internet.

The initial ONVIF implementation is a **read-only Device/Media1/PullPoint
subset**: fixed video profile, stream URI, basic virtual identity and motion
notifications. Authentication supports WS UsernameToken PasswordDigest and
HTTP Basic; **HTTP Digest is not implemented**. UDP WS-Discovery is not
implemented: use the recorder's manual ONVIF-add workflow. There is no
configuration/admin service, push callback, recording service or certification
claim. See [ONVIF details](custom_components/ha_onvif_exporter/README-ONVIF.md).

The snapshot capability is optional and is not advertised without a supplied
JPEG provider. This release does not promise a snapshot from every source.

## Install with HACS

This repository is a custom integration repository, not a listing in HACS's
default catalog.

1. Back up Home Assistant. In HACS, add the custom repository
   `https://github.com/Inkhomi86/ha-onvif-exporter`, category **Integration**.
2. Download through HACS (select `main` if no tagged release is available).
   Follow any restart/repair notice
   shown by HACS; do not assume every installation or update is hot-reloadable.
3. Open **Settings → Devices & services → Add integration → HA ONVIF Exporter**.
4. Select the compatible existing camera, an exact local IPv4 bind address,
   free ONVIF/RTSP ports, a dedicated username/password and allowed client IPs.
5. Start with **Manual only**, activate a short test window, and verify the
   output before enabling automatic triggers or relying on recordings.
6. Manually add the virtual camera in the recorder using the configured HA
   address, ONVIF port and the same dedicated credentials. The staging defaults
   are ONVIF **1986** and RTSP **18557**; they intentionally avoid common
   previously configured endpoints. Port conflicts are rejected.

For a recorder such as UniFi Protect, compatibility depends on its actual
ONVIF client. AI features, door events and timeline thumbnails are controlled
by the recorder, not guaranteed by this integration.

## Configure activations

**Manual:** use the integration's activation control or a HA automation that
invokes that control. Use this mode to establish baseline compatibility.

**Events:** configure the actual event name, a flat data filter and the field
containing its UTC timestamp. Filter fields are AND; a list at one field is OR.
Choose only events for the intended camera/device. Old or duplicate activations
are rejected. Inspect your camera integration's documented event schema instead
of copying someone else's device identifiers.

**Binary sensor:** select the intended sensor. Only a real `off` → `on`
transition activates; restored, unknown or unavailable states do not.

Options apply only to this exporter. The virtual identity remains stable when
settings change, but changing endpoints/credentials can require updating the
recorder. Stable identity is not proof that an existing recorder adoption or AI
pairing survives a migration; test it explicitly.

## Security and troubleshooting

Use a separate printable ASCII password of 16–128 characters, not a HA login or
camera password. Configuration is stored through HA's config-entry system;
protect HA backups accordingly. No personal credentials are bundled. Client
credentials are not written into public URLs or process command arguments.
WS replay protection is bounded and process-local; it does not survive a
process restart. PasswordDigest is authentication, not a signature over SOAP.
Plain HTTP Basic and RTSP require a trusted network or appropriate secure
transport. This integration does not supply TLS.

If activation fails, first verify HA live video, the managed go2rtc prerequisite,
ports/client addresses and the integration's safe status/error code. Do not
work around failure by enabling anonymous media access or widening the allowlist.
Media timeout, worker exit or unconfirmed cleanup prevents further unsafe
activation and must be resolved before using recordings.

HA's administrator diagnostics download includes safe runtime counts: active
ONVIF subscriptions, successful prepared responses and motion notifications
included in PullPoint responses. These counts do not identify clients or prove
that a recorder received a response or recorded a clip. They reset when the
exporter reloads. Integration diagnostics omit configuration, credentials,
camera/client addresses, subscription tokens and media; HA adds its normal
framework metadata separately.

Report a sanitized error code, HA version, architecture and trigger mode in
[Issues](https://github.com/Inkhomi86/ha-onvif-exporter/issues). Do not upload
passwords, raw SDP, camera URLs containing credentials, configuration dumps,
unredacted logs or HA storage/backup files.

## Development and licensing

Tests cover public synthetic fixtures, authentication, XML boundaries,
PullPoint state, owned-consumer lifetime and worker/control boundaries. They
are not end-to-end hardware acceptance tests. Validate standby/live/standby,
repeated activations, unload/restart recovery, HA camera coexistence and recorder
motion interpretation on the intended installation before relying on it.

Project source and original branding/standby assets use the [MIT License](LICENSE).
Bundled MediaMTX and runtime dependencies retain their own licenses; see
[third-party notices](THIRD_PARTY.md). Brand names belong to their respective
owners; no endorsement is implied.
