"""Parent-death guard validation with fakes; never executes/signals a process."""
from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

COMPONENT = Path(__file__).resolve().parents[1] / "custom_components" / "ha_onvif_exporter"
SPEC = importlib.util.spec_from_file_location("exporter_guard_review", COMPONENT / "process_guard.py")
guard = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(guard)


class GuardTests(unittest.TestCase):
    def run_guard(self, role="worker", parent="456", target=None, *, platform="linux",
                  parent_values=(456, 456), prctl_result=0, config=None, mode_error=False,
                  symlink=False, exec_error=False):
        target = target or str(COMPONENT / "media_worker.py")
        args = [str(COMPONENT / "process_guard.py"), role, parent, target]
        if config is not None:
            args.append(config)
        library = SimpleNamespace(prctl=unittest.mock.Mock(return_value=prctl_result))
        def fake_stat(path):
            mode = 0o600 if path.name == "mediamtx.yml" else 0o700
            return SimpleNamespace(st_uid=17 if mode_error else 16, st_mode=mode)
        with (patch.object(guard.sys, "argv", args), patch.object(guard.sys, "platform", platform),
              patch.object(guard.ctypes, "CDLL", return_value=library) as load,
              patch.object(guard.os, "getppid", side_effect=parent_values),
              patch.object(guard.os, "getpid", return_value=789), patch.object(guard.os, "geteuid", return_value=16, create=True),
              patch.object(guard.signal, "SIGKILL", 9, create=True),
              patch.object(guard.os, "kill") as kill, patch.object(guard.os, "execv", side_effect=OSError() if exec_error else None) as execute,
              patch.object(guard.Path, "is_symlink", return_value=symlink), patch.object(guard.Path, "lstat", fake_stat)):
            result = guard.main()
        return result, library.prctl, load, kill, execute

    def test_worker_arms_parent_death_before_exact_isolated_exec(self):
        result, prctl, _, kill, execute = self.run_guard()
        self.assertEqual(result, 78)  # Fake exec returns; real successful exec never does.
        prctl.assert_called_once_with(1, 9, 0, 0, 0)
        kill.assert_not_called()
        execute.assert_called_once_with(guard.sys.executable,
                                       [guard.sys.executable, "-I", "-u", str(COMPONENT / "media_worker.py")])

    def test_server_exec_requires_fixed_private_paths(self):
        target = str(COMPONENT / "runtime-fixture" / "mediamtx")
        config = str(COMPONENT / "runtime-fixture" / "mediamtx.yml")
        _, _, _, kill, execute = self.run_guard("server", target=target, config=config)
        kill.assert_not_called()
        execute.assert_called_once_with(target, [target, config])

    def test_parent_race_self_kills_without_exec(self):
        _, prctl, _, kill, execute = self.run_guard(parent_values=(456, 457))
        prctl.assert_called_once()
        kill.assert_called_once_with(789, 9)
        execute.assert_not_called()

    def test_invalid_parent_or_platform_never_arms_or_executes(self):
        for options in ({"parent": "1"}, {"parent": "４５６"}, {"parent": "456x"},
                        {"parent_values": (457,)}, {"platform": "win32"}):
            with self.subTest(options=options):
                _, prctl, load, kill, execute = self.run_guard(**options)
                load.assert_not_called()
                prctl.assert_not_called()
                kill.assert_not_called()
                execute.assert_not_called()

    def test_failed_prctl_cannot_execute(self):
        _, _, _, kill, execute = self.run_guard(prctl_result=-1)
        kill.assert_not_called()
        execute.assert_not_called()

    def test_worker_cannot_run_an_arbitrary_target_or_symlink(self):
        for options in ({"target": str(COMPONENT / "other.py")}, {"symlink": True}, {"role": "other"}):
            with self.subTest(options=options):
                _, _, _, kill, execute = self.run_guard(**options)
                kill.assert_not_called()
                execute.assert_not_called()

    def test_server_wrong_owner_or_paths_cannot_execute(self):
        target = str(COMPONENT / "runtime-fixture" / "mediamtx")
        config = str(COMPONENT / "runtime-fixture" / "mediamtx.yml")
        for options in ({"mode_error": True}, {"config": str(COMPONENT / "other" / "mediamtx.yml")},
                        {"target": str(COMPONENT / "runtime-fixture" / "other")}):
            with self.subTest(options=options):
                values = {"target": target, "config": config, **options}
                _, _, _, kill, execute = self.run_guard("server", **values)
                kill.assert_not_called()
                execute.assert_not_called()

    def test_exec_failure_is_fixed_no_sensitive_error(self):
        result, _, _, _, execute = self.run_guard(exec_error=True)
        self.assertEqual(result, 78)
        execute.assert_called_once()


if __name__ == "__main__":
    unittest.main()
