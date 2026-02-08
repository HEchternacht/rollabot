import os
from dotenv import load_dotenv


def load_config():
    """Load configuration from environment variables."""
    load_dotenv()
    
    api_key = os.getenv("TS3_API_KEY", "").strip()
    if not api_key:
        raise ValueError("TS3_API_KEY is required in .env file")
    
    return {
        "host": os.getenv("TS3_HOST", "127.0.0.1:25639"),
        "api_key": api_key,
        "server_address": os.getenv("TS3_SERVER_ADDRESS", ""),
        "nickname": os.getenv("TS3_NICKNAME", "Rollabot"),
        "client_command": os.getenv("TS3_CLIENT_COMMAND", ""),
        "pid_file": os.getenv("TS3_PID_FILE", ".tsclient.pid"),
        "debug": os.getenv("DEBUG", "false").lower() in ("true", "1", "yes"),
    }
