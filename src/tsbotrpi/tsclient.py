from __future__ import annotations

import logging
import os
import signal
import subprocess
import time
from typing import List, Optional

logger = logging.getLogger(__name__)


class TSClientProcessManager:
    def __init__(
        self,
        command: Optional[List[str]],
        workdir: Optional[str],
        pid_file: str,
    ) -> None:
        self.command = command
        self.workdir = workdir
        self.pid_file = pid_file

    def _read_pid(self) -> Optional[int]:
        if not os.path.exists(self.pid_file):
            return None
        try:
            with open(self.pid_file, "r", encoding="utf-8") as handle:
                raw = handle.read().strip()
            return int(raw)
        except (OSError, ValueError):
            return None

    def _write_pid(self, pid: int) -> None:
        with open(self.pid_file, "w", encoding="utf-8") as handle:
            handle.write(str(pid))

    def _clear_pid(self) -> None:
        try:
            if os.path.exists(self.pid_file):
                os.remove(self.pid_file)
        except OSError:
            logger.debug("Failed to remove pid file: %s", self.pid_file)

    def _pid_exists(self, pid: int) -> bool:
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        return True

    def is_running(self) -> bool:
        pid = self._read_pid()
        return bool(pid and self._pid_exists(pid))

    def start(self) -> bool:
        if not self.command:
            logger.info("TS client command not configured; skipping start")
            return False
        if self.is_running():
            logger.info("TS client already running")
            return True

        creationflags = 0
        start_new_session = True
        if os.name == "nt":
            creationflags = subprocess.CREATE_NEW_PROCESS_GROUP
            start_new_session = False

        logger.info("Starting TS client: %s", " ".join(self.command))
        process = subprocess.Popen(
            self.command,
            cwd=self.workdir or None,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=creationflags,
            start_new_session=start_new_session,
        )
        self._write_pid(process.pid)
        return True

    def stop(self, timeout: float = 5.0) -> None:
        pid = self._read_pid()
        if not pid:
            return

        if not self._pid_exists(pid):
            self._clear_pid()
            return

        logger.info("Stopping TS client with pid %s", pid)
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError:
            self._clear_pid()
            return

        deadline = time.time() + timeout
        while time.time() < deadline:
            if not self._pid_exists(pid):
                self._clear_pid()
                return
            time.sleep(0.2)

        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            pass
        self._clear_pid()

    def restart(self) -> None:
        self.stop()
        self.start()
