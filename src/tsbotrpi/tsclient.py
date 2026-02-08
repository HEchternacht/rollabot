import logging
import os
import signal
import subprocess
import time
try:
    import psutil
except ImportError:
    psutil = None

logger = logging.getLogger(__name__)


class TSClientManager:
    """Manages TeamSpeak client process lifecycle."""

    def __init__(self, command, pid_file=".tsclient.pid"):
        self.command = command
        self.pid_file = pid_file
        self.terminal_pid = None
        self.last_boot_time = None

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
        if psutil:
            try:
                return psutil.pid_exists(pid)
            except:
                pass
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False

    def _find_ts_process(self):
        """Find TeamSpeak client process using psutil."""
        if not psutil:
            return None
        
        try:
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    cmdline = proc.info.get('cmdline', [])
                    if cmdline:
                        cmdline_str = ' '.join(cmdline).lower()
                        # Look for teamspeak in command line
                        if 'teamspeak' in cmdline_str or 'ts3client' in cmdline_str:
                            return proc.info['pid']
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
        except Exception as e:
            logger.error("Error finding TS process: %s", e)
        return None

    def start(self):
        """Start TeamSpeak client in terminal."""
        if not self.command:
            logger.info("No client command configured")
            return

        # Check if already running
        pid = self._read_pid()
        if pid and self._is_running(pid):
            logger.info("TS client already running (PID %s)", pid)
            return

        logger.info("Starting TS client: %s", self.command)
        
        ts_pid = None
        
        # Linux: Use x-terminal-emulator
        if os.name != "nt":
            terminal_cmds = [
                ['x-terminal-emulator', '-e', 'bash', '-c', self.command],
                ['gnome-terminal', '--', 'bash', '-c', self.command],
                ['konsole', '-e', 'bash', '-c', self.command],
                ['xterm', '-e', 'bash', '-c', self.command]
            ]
            
            for term_cmd in terminal_cmds:
                try:
                    process = subprocess.Popen(term_cmd)
                    self.terminal_pid = process.pid
                    logger.info("Started terminal (PID %s)", self.terminal_pid)
                    
                    # Wait for TS client to start, then find its PID
                    time.sleep(3)
                    ts_pid = self._find_ts_process()
                    if ts_pid:
                        logger.info("Found TS client process (PID %s)", ts_pid)
                    else:
                        logger.warning("Could not find TS client PID, using terminal PID")
                        ts_pid = self.terminal_pid
                    break
                except FileNotFoundError:
                    continue
            else:
                # No terminal found, run directly
                logger.warning("No terminal emulator found, running directly")
                process = subprocess.Popen(
                    self.command,
                    shell=True,
                    start_new_session=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                ts_pid = process.pid
        else:
            # Windows
            process = subprocess.Popen(
                self.command,
                shell=True,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            ts_pid = process.pid
        
        if ts_pid:
            self._write_pid(ts_pid)
            self.last_boot_time = time.time()
            logger.info("TS client started (PID %s)", ts_pid)

    def stop(self, timeout=5):
        """Stop TeamSpeak client and terminal."""
        pid = self._read_pid()
        if not pid:
            return

        if not self._is_running(pid):
            if os.path.exists(self.pid_file):
                os.remove(self.pid_file)
            return

        logger.info("Stopping TS client (PID %s)", pid)
        
        # Kill TS client process
        if psutil:
            try:
                proc = psutil.Process(pid)
                proc.terminate()
                proc.wait(timeout=timeout)
                logger.info("TS client terminated gracefully")
            except psutil.TimeoutExpired:
                logger.warning("TS client didn't terminate, force killing...")
                try:
                    proc.kill()
                    proc.wait(timeout=2)
                except:
                    pass
            except psutil.NoSuchProcess:
                logger.debug("Process already dead")
            except Exception as e:
                logger.error("Error stopping TS client: %s", e)
        else:
            # Fallback without psutil
            try:
                os.kill(pid, signal.SIGTERM)
                deadline = time.time() + timeout
                while time.time() < deadline:
                    if not self._is_running(pid):
                        break
                    time.sleep(0.2)
                if self._is_running(pid):
                    os.kill(pid, signal.SIGKILL)
            except OSError:
                pass
        
        # Kill terminal if different
        if self.terminal_pid and self.terminal_pid != pid:
            try:
                if psutil:
                    terminal_proc = psutil.Process(self.terminal_pid)
                    terminal_proc.terminate()
                else:
                    os.kill(self.terminal_pid, signal.SIGTERM)
            except:
                pass
        
        if os.path.exists(self.pid_file):
            os.remove(self.pid_file)
        self.terminal_pid = None

    def restart(self):
        """Restart TeamSpeak client (with 1 minute cooldown).
        
        Returns:
            bool: True if restart was performed, False if skipped due to cooldown
        """
        # Check if enough time has passed since last boot (box64 takes ~1 minute)
        if self.last_boot_time:
            elapsed = time.time() - self.last_boot_time
            if elapsed < 60:
                logger.warning("Skipping restart - only %.1fs since last boot (need 60s)", elapsed)
                return False
        
        logger.info("Restarting TS client")
        self.stop()
        time.sleep(2)
        self.start()
        return True
