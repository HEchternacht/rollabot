TSBOTRPI
========

TeamSpeak bot controlled by ClientQuery, with optional TeamSpeak client process
management.

Setup
-----
1) Copy .env.example to .env and fill values.
2) Install dependencies with uv:

	uv sync

Run
---
Run the bot entrypoint:

	uv run python ini.py

Notes
-----
- The bot reconnects on disconnects.
- If connection is refused, the TeamSpeak client is restarted when
	TS3_CLIENT_COMMAND is set.
- ClientQuery commands require TS3_CLIENTQUERY_API_KEY.
