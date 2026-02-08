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
   
   # Optional - Server connection
   TS3_SERVER_ADDRESS=#ascendedauroria
   TS3_NICKNAME=Rollabot
   
   # Debug mode
   DEBUG=false
   
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
  ├── commands.py   # Command handlers (edit here to add/modify commands!)
  ├── config.py     # Environment configuration
  └── tsclient.py   # TS client process manager
```

## Adding Commands

Edit [src/tsbotrpi/commands.py](src/tsbotrpi/commands.py) to add or modify bot commands. The `process_command` function receives:
- `bot`: TS3Bot instance with methods like `masspoke()`, `add_hunted()`, etc.
- `msg`: The message text
- `nickname`: The sender's nickname

Example:
```python
if msg.startswith("!hello"):
    return f"Hello {nickname}!"
```

## How It Works

1. Bot attempts to connect to TeamSpeak ClientQuery
2. **If connection refused/unavailable**: Automatically starts TS client in new terminal
3. **Event handling thread**: Listens for messages without timeout in separate thread
4. **Main thread**: Manages connection and sends keepalive every 3 seconds
5. On disconnect: automatically reconnects
6. Uses psutil to find actual TS client PID (searches for 'teamspeak' or 'ts3client' in process list)

## Debug Mode

Set `DEBUG=true` in [.env](.env) to enable verbose logging:
```bash
DEBUG=true
```

This will show:
- All debug messages
- x3tBot message filtering
- Connection attempts
- Thread lifecycle

## Notes

- Requires TeamSpeak ClientQuery API key
- Designed for Raspberry Pi but works on any platform
- **TS client only starts when connection fails** (not on bot startup)
- Uses `x-terminal-emulator` (or fallback to gnome-terminal, konsole, xterm)
- Requires `psutil` for proper process tracking
- Tracks actual TS client PID, not just terminal PID
- Process management handles crashes and restarts
- Logs to stdout with timestamps
