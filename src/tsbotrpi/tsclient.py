import logging
import os
import signal
import subprocess
import time

logger = logging.getLogger(__name__)


class TSClientManager:
    """Manages TeamSpeak client process lifecycle."""

    def __init__(self, command, pid_file=".tsclient.pid"):
        self.command = command
        self.pid_file = pid_file

    def _read_pid(self):
        """Read PID from file."""
        if not os.path.exists(self.pid_file):
            return None
        try:
            with open(self.pid_file, "r") as f:
                return int(f.read().strip())
        except (OSError, ValueError):
            return None

    def _write_pid(self, pid):
        """Write PID to file."""
        with open(self.pid_file, "w") as f:
            f.write(str(pid))

    def _is_running(self, pid):
        """Check if process is running."""
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False

    def start(self):
        """Start TeamSpeak client."""
        if not self.command:
            logger.info("No client command configured")
            return

        # Check if already running
        pid = self._read_pid()
        if pid and self._is_running(pid):
            logger.info("TS client already running (PID %s)", pid)
            return

        logger.info("Starting TS client: %s", self.command)
        
        # Start process
        if os.name == "nt":
            process = subprocess.Popen(
                self.command,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        else:
            process = subprocess.Popen(
                self.command,
                start_new_session=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        
        self._write_pid(process.pid)
        logger.info("TS client started (PID %s)", process.pid)

    def stop(self, timeout=5):
        """Stop TeamSpeak client."""
        pid = self._read_pid()
        if not pid:
            return

        if not self._is_running(pid):
            os.remove(self.pid_file)
            return

        logger.info("Stopping TS client (PID %s)", pid)
        
        # Try SIGTERM first
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError:
            os.remove(self.pid_file)
            return

        # Wait for graceful shutdown
        deadline = time.time() + timeout
        while time.time() < deadline:
            if not self._is_running(pid):
                os.remove(self.pid_file)
                return
            time.sleep(0.2)

        # Force kill if still running
        try:
            os.kill(pid, signal.SIGKILL)
            logger.warning("Force killed TS client")
        except OSError:
            pass
        
        os.remove(self.pid_file)

    def restart(self):
        """Restart TeamSpeak client."""
        logger.info("Restarting TS client")
        self.stop()
        time.sleep(1)
        self.start()
