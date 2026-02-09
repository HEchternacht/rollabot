"""
Command handlers for the TS3 bot.
Edit this file to add/modify bot commands.
"""
import logging
import csv
import os

logger = logging.getLogger(__name__)

import requests


def get_txt():
    api_url="https://xinga-me.appspot.com/api"
    try:
        response=requests.get(api_url)
        return response.json()['xingamento']
    except Exception as e:
        return None


def search_activity_log(search_term: str, max_results: int = 10):
    """
    Search activity log for entries matching uid, nickname, or ip.
    
    Args:
        search_term: The uid, nickname, or ip to search for
        max_results: Maximum number of results to return
    
    Returns:
        str: Formatted results or error message
    """
    try:
        # Get log file path (in project root)
        log_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        log_path = os.path.join(log_dir, 'activities_log.csv')
        
        if not os.path.exists(log_path):
            return "Activity log not found. No events have been logged yet."
        
        matches = []
        with open(log_path, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                # Case-insensitive search in nickname, uid, or ip
                if (search_term.lower() in row.get('nickname', '').lower() or
                    search_term.lower() in row.get('uid', '').lower() or
                    search_term in row.get('ip', '')):
                    matches.append(row)
                    
                    if len(matches) >= max_results:
                        break
        
        if not matches:
            return f"No activity found for: {search_term}"
        
        # Format results
        result = f"Found {len(matches)} activities for '{search_term}':\\n"
        for i, match in enumerate(matches, 1):
            timestamp = match.get('timestamp', '')[:19]  # Truncate microseconds
            event_type = match.get('event_type', 'unknown')
            nickname = match.get('nickname', 'N/A')
            details = match.get('details', '')[:100]  # Limit details length
            
            result += f"{i}. [{timestamp}] {event_type} - {nickname}\\n"
            if details:
                result += f"   Details: {details}\\n"
        
        return result
        
    except Exception as e:
        logger.error(f"Error searching activity log: {e}")
        return f"Error searching log: {str(e)}"


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
    
    if msg.startswith("!help"):
        return (
            "Available commands:\n"
            "!help - Show this message\n"
            "!mp <message> - Poka todo mundo , util se o x3tbot estiver offline\n"
            "!snapshot - Get detailed client snapshot\n"
            "!logger <uid/nickname/ip> - Search activity log for user\n"
        )





    # Mass poke command
    if msg.startswith("!mp"):
        bot.masspoke(f"{nickname} te cutucou: {msg[4:]}")
        return "Poking all clients..."
    
    # Add to hunted list (via x3tBot)#hide from help since it's x3tBot specific
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
    
    # Search activity log
    if msg.startswith("!logger"):
        search_term = msg[7:].strip()
        if not search_term:
            return "Usage: !logger <uid/nickname/ip>"
        return search_activity_log(search_term)
    
    # Unknown command




    t=get_txt()
    if isinstance(t, str) and t.strip():
        t=f"\n{t.strip()}"
    else:
        t=""
    default_response = f"esse nao é o x3tbot, digite !help para ver os comandos disponiveis {t}"
    
    return str(default_response)
