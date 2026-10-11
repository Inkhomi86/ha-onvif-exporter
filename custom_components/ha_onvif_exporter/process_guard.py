"""Linux child-only parent-death guard. No credentials, URLs or logs."""
from __future__ import annotations

import ctypes
import os
from pathlib import Path
import signal
import stat
import sys


def main() -> int:
    try:
        if not sys.platform.startswith("linux") or len(sys.argv) not in (4, 5):
            return 78
        role, parent_text, target_text = sys.argv[1:4]
        if not parent_text.isascii() or not parent_text.isdecimal():
            return 78
        expected_parent = int(parent_text)
        if expected_parent <= 1 or os.getppid() != expected_parent:
            return 78
        libc = ctypes.CDLL(None, use_errno=True)
        if libc.prctl(1, signal.SIGKILL, 0, 0, 0) != 0:  # PR_SET_PDEATHSIG
            return 78
        if os.getppid() != expected_parent:
            os.kill(os.getpid(), signal.SIGKILL)
            return 78
        target = Path(target_text)
        if not target.is_absolute() or target.is_symlink():
            return 78
        if role == "worker" and len(sys.argv) == 4:
            if target != Path(__file__).with_name("media_worker.py"):
                return 78
            os.execv(sys.executable, [sys.executable, "-I", "-u", str(target)])
        elif role == "server" and len(sys.argv) == 5:
            config = Path(sys.argv[4])
            if not config.is_absolute() or config.is_symlink() or target.name != "mediamtx" or config.name != "mediamtx.yml" or target.parent != config.parent:
                return 78
            for path, mode in ((target.parent, 0o700), (target, 0o700), (config, 0o600)):
                info = path.lstat()
                if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != mode:
                    return 78
            os.execv(str(target), [str(target), str(config)])
        return 78
    except Exception:
        return 78


if __name__ == "__main__":
    raise SystemExit(main())
