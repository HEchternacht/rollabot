from __future__ import annotations

import logging
import time
from typing import Callable, Optional

import ts3

from .config import Settings
from .tsclient import TSClientProcessManager

logger = logging.getLogger(__name__)


AnswerFunc = Callable[["TS3Bot", str, str], Optional[str]]


class TS3Bot:
    def __init__(
        self,
        settings: Settings,
        process_manager: Optional[TSClientProcessManager] = None,
        answer_function: Optional[AnswerFunc] = None,
    ) -> None:
        self.settings = settings
        self.process_manager = process_manager
        self.answer_function = answer_function or TS3Bot._answer
        self.client_conn: Optional[ts3.query.TS3ClientConnection] = None

    def _is_connection_refused(self, exc: Exception) -> bool:
        text = str(exc).lower()
        return "refused" in text or "10061" in text or "111" in text

    def _maybe_restart_tsclient(self) -> None:
        if not self.process_manager:
            return
        logger.warning("Restarting TS client process")
        self.process_manager.restart()

    def _connect_clientquery(self) -> ts3.query.TS3ClientConnection:
        conn = ts3.query.TS3ClientConnection(self.settings.clientquery_addr)
        conn.auth(apikey=self.settings.clientquery_api_key)
        conn.clientnotifyregister(event="notifytextmessage", schandlerid=1)
        return conn

    def _ensure_clientquery(self) -> bool:
        if self.client_conn:
            return True
        try:
            self.client_conn = self._connect_clientquery()
            return True
        except Exception as exc:
            logger.error("ClientQuery connect failed: %s", exc)
            if self._is_connection_refused(exc):
                self._maybe_restart_tsclient()
            return False

    def _get_xbot(self) -> Optional[dict]:
        if not self._ensure_clientquery():
            return None
        clients = self.client_conn.clientlist().parsed
        matches = [
            client
            for client in clients
            if self.settings.xbot_nickname_contains in client.get("client_nickname", "")
        ]
        return matches[0] if matches else None

    def add_hunted(self, target: str) -> str:
        if not self._ensure_clientquery():
            return "ClientQuery not connected"
        xbot = self._get_xbot()
        if not xbot:
            return "x3tBot Auroria not found"
        self.client_conn.sendtextmessage(
            targetmode=1,
            target=xbot["clid"],
            msg=f"!hunted add {target}",
        )

        lines_left = self.settings.response_wait_lines
        while lines_left > 0:
            try:
                event = self.client_conn.wait_for_event(
                    timeout=self.settings.response_wait_timeout
                )
            except ts3.query.TS3TimeoutError:
                break
            if event.parsed:
                logger.info("ClientQuery response: %s", event.parsed)
            lines_left -= 1

        return f"Added {target} to hunted list."

    def masspoke(self, msg: str) -> None:
        if not self._ensure_clientquery():
            return
        clids = [client["clid"] for client in self.client_conn.clientlist().parsed]
        for clid in clids:
            self.client_conn.clientpoke(msg=msg, clid=clid)

    def _answer(self, input_str: str, nickname: str) -> Optional[str]:
        if input_str.startswith("!mp"):
            self.masspoke(nickname + " te cutucou :" + input_str[4:])
            return "Poking all clients..."
        if input_str.startswith("!hunted add"):
            target = input_str[12:].strip()
            return self.add_hunted(target)
        if input_str.startswith("!snapshot"):
            if not self._ensure_clientquery():
                return "ClientQuery not connected"
            snapshot = self.client_conn.clientlist(
                info=True,
                country=True,
                uid=True,
                ip=True,
                groups=True,
                times=True,
                voice=True,
                away=True,
            ).parsed
            return str(snapshot)
        return "Unknown command. Input was: " + input_str

    def run(self) -> None:
        while True:
            if not self._ensure_clientquery():
                time.sleep(self.settings.reconnect_delay)
                continue

            try:
                event = self.client_conn.wait_for_event(timeout=self.settings.event_timeout)
                if event.parsed:
                    msg = event.parsed[0].get("msg", "")
                    clid = event.parsed[0].get("invokerid")
                    nickname = event.parsed[0].get("invokername", "")
                    response = self.answer_function(self, msg, nickname)
                    if response:
                        self.client_conn.sendtextmessage(
                            targetmode=1, target=clid, msg=response
                        )
            except ts3.query.TS3TimeoutError:
                pass
            except Exception as exc:
                logger.error("ClientQuery loop error: %s", exc)
                if self._is_connection_refused(exc):
                    self._maybe_restart_tsclient()
                self.client_conn = None
                time.sleep(self.settings.reconnect_delay)
            finally:
                if self.client_conn:
                    self.client_conn.send_keepalive()



if __name__ == "__main__":
    import sys
    from .config import load_settings

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    settings = load_settings()
    bot = TS3Bot(settings=settings)
    try:
        bot.run()
    except KeyboardInterrupt:
        sys.exit(0)