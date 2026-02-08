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
        "client_command": os.getenv("TS3_CLIENT_COMMAND", ""),
        "pid_file": os.getenv("TS3_PID_FILE", ".tsclient.pid"),
    }
