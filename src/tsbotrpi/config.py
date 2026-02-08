from __future__ import annotations

import os
import shlex
from dataclasses import dataclass
from typing import List, Optional

from dotenv import load_dotenv # type: ignore


@dataclass(frozen=True)
class Settings:
    clientquery_addr: str
    clientquery_api_key: str
    xbot_nickname_contains: str
    client_command: Optional[List[str]]
    client_workdir: Optional[str]
    client_pid_file: str
    reconnect_delay: float
    event_timeout: float
    response_wait_lines: int
    response_wait_timeout: float


def _parse_command(value: str) -> Optional[List[str]]:
    if not value:
        return None
    return shlex.split(value)


def load_settings() -> Settings:
    load_dotenv()

    clientquery_addr = os.getenv("TS3_CLIENTQUERY_ADDR", "127.0.0.1:25639")
    clientquery_api_key = os.getenv("TS3_CLIENTQUERY_API_KEY", "").strip()
    xbot_nickname_contains = os.getenv("TS3_XBOT_NICKNAME", "x3tBot Auroria")

    client_command = _parse_command(os.getenv("TS3_CLIENT_COMMAND", "").strip())
    client_workdir = os.getenv("TS3_CLIENT_WORKDIR")
    client_pid_file = os.getenv("TS3_CLIENT_PID_FILE", ".tsclient.pid")

    reconnect_delay = float(os.getenv("TS3_RECONNECT_DELAY", "1.0"))
    event_timeout = float(os.getenv("TS3_EVENT_TIMEOUT", "3.0"))
    response_wait_lines = int(os.getenv("TS3_RESPONSE_LINES", "10"))
    response_wait_timeout = float(os.getenv("TS3_RESPONSE_TIMEOUT", "1.0"))

    if not clientquery_api_key:
        raise ValueError("TS3_CLIENTQUERY_API_KEY is required")

    return Settings(
        clientquery_addr=clientquery_addr,
        clientquery_api_key=clientquery_api_key,
        xbot_nickname_contains=xbot_nickname_contains,
        client_command=client_command,
        client_workdir=client_workdir,
        client_pid_file=client_pid_file,
        reconnect_delay=reconnect_delay,
        event_timeout=event_timeout,
        response_wait_lines=response_wait_lines,
        response_wait_timeout=response_wait_timeout,
    )
