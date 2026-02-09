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


def search_activity_log(search_term: str, max_results: int = 15):
    """
    Search human-readable activity log for entries matching uid, nickname, or ip.
    
    Args:
        search_term: The uid, nickname, or ip to search for
        max_results: Maximum number of results to return
    
    Returns:
        str: Formatted results or error message
    """
    try:
        # Get log file paths (in project root)
        log_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        readable_log_path = os.path.join(log_dir, 'activity_log_readable.csv')
        users_seen_path = os.path.join(log_dir, 'users_seen.csv')
        
        if not os.path.exists(readable_log_path):
            return "Activity log not found. No events have been logged yet."
        
        # First, try to find UID from nickname or IP in users_seen.csv
        target_uids = set()
        matched_user_info = {}  # uid -> (nickname, ip)
        
        if os.path.exists(users_seen_path):
            with open(users_seen_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    uid = row.get('UID', '')
                    nickname = row.get('NICKNAME', '')
                    ip = row.get('IP', '')
                    
                    # Check if search term matches UID, nickname, or IP
                    if (search_term.lower() in uid.lower() or
                        search_term.lower() in nickname.lower() or
                        search_term in ip):
                        target_uids.add(uid)
                        matched_user_info[uid] = (nickname, ip)
        else:
            # If users_seen.csv doesn't exist, assume search_term is a UID
            target_uids.add(search_term)
        
        if not target_uids:
            return f"No user found matching: {search_term}"
        
        # Now search the readable activity log for these UIDs
        matches = []
        with open(readable_log_path, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                uid = row.get('UID', '')
                if uid in target_uids:
                    matches.append(row)
                    
                    if len(matches) >= max_results:
                        break
        
        if not matches:
            return f"No activity found for: {search_term}"
        
        # Format results
        user_display = search_term
        if len(target_uids) == 1:
            uid = list(target_uids)[0]
            if uid in matched_user_info:
                nickname, ip = matched_user_info[uid]
                user_display = f"{nickname} ({uid[:8]}...)"
        
        result = f"Found {len(matches)} activities for '{user_display}':\\n"
        for i, match in enumerate(matches, 1):
            timestamp = match.get('TIMESTAMP', '')
            event = match.get('EVENT', 'unknown event')
            
            result += f"{i}. [{timestamp}] {event}\\n"
        
        if len(matches) == max_results:
            result += f"\\n(Showing first {max_results} results)"
        
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
