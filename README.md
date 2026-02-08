# TSBOTRPI

TeamSpeak bot for Raspberry Pi with ClientQuery control and automatic TS client management.

## Features

- **Smart client management**: Only starts TS client when connection refused/unavailable
- **Auto-reconnect**: Automatically reconnects on disconnects
- **Process tracking**: Uses psutil to track actual TS client PID (not just terminal)
- **Terminal spawning**: Uses x-terminal-emulator on Raspberry Pi/Linux
- **Graceful shutdown**: Properly kills both terminal and TS client processes
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

For Raspberry Pi with automatic TS client management:

1. Set `TS3_CLIENT_COMMAND` in .env:
   ```bash
   # Your actual command from .env
   TS3_CLIENT_COMMAND=sudo box64 /home/pi/Downloads/TeamSpeak3-Client-linux_amd64/ts3client_linux_amd64 --no-sandbox
   ```

2. The bot will:
   - **Only start TS client when needed** (connection refused/unavailable)
   - Spawn TS client in x-terminal-emulator
   - Track actual TS client PID using psutil (not just terminal PID)
   - Kill old processes before starting new ones
   - Cleanup on bot shutdown

3. Install psutil for proper process tracking:
   ```bash
   pip install psutil
   ```

## Architecture

```
ini.py              # Main entry point
src/tsbotrpi/
  ├── bot.py        # Core bot logic (AutoanswerScheduler pattern)
  ├── cattempts to connect to TeamSpeak ClientQuery
2. **If connection refused/unavailable**: Automatically starts TS client in new terminal
3. Registers for text message notifications
4. Listens for messages and processes commands
5. On disconnect: automatically reconnects
6. Uses psutil to find actual TS client PID (searches for 'teamspeak' or 'ts3client' in process list)
7
1. Bot connects to TeamSpeak ClientQuery
2. Registers for text message notifications
3. Listens for messages and processes commands
4. On connection refused: restarts TS client (if configured)
5. On disconnect: automatically reconnects
6. Sends keepalive every 3 seconds

## Notes

- Requires TeamSpeak ClientQuery API key
- **TS client only starts when connection fails** (not on bot startup)
- Uses `x-terminal-emulator` (or fallback to gnome-terminal, konsole, xterm)
- Requires `psutil` for proper process tracking
- Tracks actual TS client PID, not just terminal PIDrm
- Uses `x-terminal-emulator` for TS client on Linux
- Process management handles crashes and restarts
- Logs to stdout with timestamps
