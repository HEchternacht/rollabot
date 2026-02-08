# TSBOTRPI

TeamSpeak bot for Raspberry Pi with ClientQuery control and automatic TS client management.

## Features

- **Auto-reconnect**: Automatically reconnects on disconnects
- **TS client restart**: Restarts TeamSpeak client on connection refused errors
- **Process management**: Manages TS client process lifecycle with PID tracking
- **Command support**:
  - `!mp <message>` - Mass poke all clients
  - `!hunted add <target>` - Add target to x3tBot hunted list
  - `!snapshot` - Get detailed client list snapshot

## Setup

1. **Clone and install**:
   ```bash
   git clone <repo>
   cd TSBOTRPI
   ```

2. **Create .env file**:
   ```bash
   cp .env.example .env
   ```

3. **Configure .env**:
   ```bash
   # Required
   TS3_HOST=127.0.0.1:25639
   TS3_API_KEY=YOUR-API-KEY-HERE
   
   # Optional - for Raspberry Pi TS client management
   # Example: x-terminal-emulator -e /opt/teamspeak3-client/ts3client_runscript.sh
   TS3_CLIENT_COMMAND=
   TS3_PID_FILE=.tsclient.pid
   ```

4. **Install dependencies**:
   ```bash
   uv sync
   # or
   pip install -r requirements.txt
   ```

## Running

**Main entry point** (recommended):
```bash
uv run python ini.py
# or
python ini.py
```

**Alternative entry point**:
```bash
python main.py
```

## Raspberry Pi Setup

For Raspberry Pi with TS client management:

1. Set `TS3_CLIENT_COMMAND` in .env:
   ```bash
   TS3_CLIENT_COMMAND=x-terminal-emulator -e /path/to/teamspeak3-client
   ```

2. The bot will:
   - Start TS client on launch
   - Track process PID in `.tsclient.pid`
   - Restart client on connection refused
   - Kill old process before starting new one
   - Cleanup on bot shutdown

## Architecture

```
ini.py              # Main entry point
src/tsbotrpi/
  ├── bot.py        # Core bot logic (AutoanswerScheduler pattern)
  ├── config.py     # Environment configuration
  └── tsclient.py   # TS client process manager
```

## How It Works

1. Bot connects to TeamSpeak ClientQuery
2. Registers for text message notifications
3. Listens for messages and processes commands
4. On connection refused: restarts TS client (if configured)
5. On disconnect: automatically reconnects
6. Sends keepalive every 3 seconds

## Notes

- Requires TeamSpeak ClientQuery API key
- Designed for Raspberry Pi but works on any platform
- Uses `x-terminal-emulator` for TS client on Linux
- Process management handles crashes and restarts
- Logs to stdout with timestamps
