# Third-party notices

The project MIT license applies to its original source, branding and generated
STANDBY media. It does not replace licenses of bundled software, runtime
dependencies, Home Assistant, go2rtc or recorder software.

## Bundled MediaMTX 1.21.2

The integration includes the unmodified official Linux amd64 release archive:

- Project: <https://github.com/bluenviron/mediamtx>
- Release: <https://github.com/bluenviron/mediamtx/releases/tag/v1.21.2>
- Archive: `mediamtx_v1.21.2_linux_amd64.tar.gz`
- SHA256: `121be6e00397e473a95d83c75f4fcd3819a6af3b0e1a50889cba43ab4fab6a27`
- The archive retains the upstream `LICENSE`; its text is also reproduced below.
- Upstream source and dependencies remain available from the versioned project.

```text
MIT License

Copyright (c) 2019 aler9

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

## Python runtime dependencies, not vendored source

These projects are imported from the installed HA/Python environment or resolved
by HA's dependency management. Their source/wheels are not bundled in this
integration repository. Versions and compatibility are controlled by the
integration manifest and the supported HA runtime, not by these notices.

| Project | Upstream license/reference |
| --- | --- |
| aiortc | [BSD 3-Clause](https://github.com/aiortc/aiortc/blob/1.15.0/LICENSE), copyright Jeremy Lainé |
| PyAV | [BSD 3-Clause](https://github.com/PyAV-Org/PyAV/blob/v17.0.1/LICENSE.txt), original committers |
| aiohttp | [Apache 2.0](https://github.com/aio-libs/aiohttp/blob/v3.14.3/LICENSE.txt), aio-libs contributors |
| defusedxml | [PSF License v2](https://github.com/tiran/defusedxml/blob/v0.7.1/LICENSE), Python Software Foundation |

PyAV uses FFmpeg libraries and optional codecs from its installed build. Their
licensing is not replaced by this project's MIT license. See the installed
distribution's notices and [FFmpeg's licensing information](https://ffmpeg.org/legal.html)
before redistributing a Python/runtime image or additional codec binaries.
This repository does not bundle a PyAV wheel or an FFmpeg executable.

## Original assets and protocol references

`assets/standby.mp4` is project-generated black/white title media, not footage
from a real camera. Its SHA256 is
`fbdeec17613eac45ea6794b664286a57e268bb79ebef032ccb73911fe78387c8`.
The `brand/` icon is an original geometric camera/export drawing under the
project MIT license; it is not a Home Assistant, ONVIF, BTicino or UniFi logo.

ONVIF WSDLs and the OASIS UsernameToken specification were consulted as protocol
references. No third-party ONVIF implementation source is vendored. These
standards retain their own notices and trademarks; consulting or implementing
a subset does not confer certification.
