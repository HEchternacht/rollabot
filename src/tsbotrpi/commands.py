"""
Command handlers for the TS3 bot.
Edit this file to add/modify bot commands.
"""
import logging

logger = logging.getLogger(__name__)


def process_command(bot, msg, nickname):
    """
    Process incoming messages and return response.
    
    Args:
        bot: TS3Bot instance (has methods like masspoke, add_hunted, etc.)
        msg: Message text from user
        nickname: Nickname of user who sent message
    
    Returns:
        str: Response to send back to user
    """
    
    # Mass poke command
    if msg.startswith("!mp"):
        bot.masspoke(f"{nickname} te cutucou: {msg[4:]}")
        return "Poking all clients..."
    
    # Add to hunted list (via x3tBot)
    if msg.startswith("!hunted add"):
        target = msg[12:].strip()
        return bot.add_hunted(target)
    
    # Get detailed client snapshot
    if msg.startswith("!snapshot"):
        snapshot = bot.conn.clientlist(
            info=True, country=True, uid=True, ip=True,
            groups=True, times=True, voice=True, away=True
        ).parsed
        return str(snapshot)
    
    # Unknown command
    return f"Unknown command: {msg}"
