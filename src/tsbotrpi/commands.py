"""
Command handlers for the TS3 bot.
Edit this file to add/modify bot commands.
"""
import logging
import csv
import os
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

import requests


def register_exp_user(uid: str) -> str:
    """
    Register a user for guild exp notifications.
    
    Args:
        uid: User UID to register
    
    Returns:
        str: Success or error message
    """
    try:
        log_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        registered_file = os.path.join(log_dir, 'registered.txt')
        
        # Load existing UIDs
        registered_uids = set()
        if os.path.exists(registered_file):
            with open(registered_file, 'r', encoding='utf-8') as f:
                registered_uids = set(line.strip() for line in f if line.strip())
        
        # Check if already registered
        if uid in registered_uids:
            return "You are already registered for guild exp notifications."
        
        # Add new UID
        registered_uids.add(uid)
        
        # Write back to file
        with open(registered_file, 'w', encoding='utf-8') as f:
            for registered_uid in sorted(registered_uids):
                f.write(f"{registered_uid}\n")
        
        logger.info(f"Registered {uid} for guild exp notifications")
        return "Successfully registered for guild exp notifications!"
        
    except Exception as e:
        logger.error(f"Error registering user for exp notifications: {e}")
        return f"Error registering: {str(e)}"


def unregister_exp_user(uid: str) -> str:
    """
    Unregister a user from guild exp notifications.
    
    Args:
        uid: User UID to unregister
    
    Returns:
        str: Success or error message
    """
    try:
        log_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        registered_file = os.path.join(log_dir, 'registered.txt')
        
        if not os.path.exists(registered_file):
            return "You are not registered for guild exp notifications."
        
        # Load existing UIDs
        registered_uids = set()
        with open(registered_file, 'r', encoding='utf-8') as f:
            registered_uids = set(line.strip() for line in f if line.strip())
        
        # Check if registered
        if uid not in registered_uids:
            return "You are not registered for guild exp notifications."
        
        # Remove UID
        registered_uids.remove(uid)
        
        # Write back to file
        with open(registered_file, 'w', encoding='utf-8') as f:
            for registered_uid in sorted(registered_uids):
                f.write(f"{registered_uid}\n")
        
        logger.info(f"Unregistered {uid} from guild exp notifications")
        return "Successfully unregistered from guild exp notifications."
        
    except Exception as e:
        logger.error(f"Error unregistering user from exp notifications: {e}")
        return f"Error unregistering: {str(e)}"


def get_txt():
    api_url="https://xinga-me.appspot.com/api"
    try:
        response=requests.get(api_url)
        return response.json()['xingamento']
    except Exception as e:
        return None


def format_snapshot(clients_data):
    """
    Format client snapshot data in human-readable format.
    
    Args:
        clients_data: List of client dicts from clientlist()
    
    Returns:
        str: Formatted snapshot
    """
    if not clients_data:
        return "No clients connected."
    
    result = f"Connected Clients ({len(clients_data)}):\n"
    result += "=" * 50 + "\n\n"
    
    for i, client in enumerate(clients_data, 1):
        nickname = client.get('client_nickname', 'Unknown')
        uid = client.get('client_unique_identifier', 'N/A')[:16] + '...'  # Truncate UID
        country = client.get('client_country', 'N/A')
        
        # Status indicators
        away = '💤 Away' if client.get('client_away') == '1' else '✅ Active'
        input_muted = '🔇' if client.get('client_input_muted') == '1' else '🎤'
        output_muted = '🔈' if client.get('client_output_muted') == '1' else '🔊'
        talking = '🗣️' if client.get('client_flag_talking') == '1' else ''
        
        result += f"{i}. {nickname} [{country}]\n"
        result += f"   UID: {uid}\n"
        result += f"   Status: {away} | Mic: {input_muted} | Speaker: {output_muted} {talking}\n"
        result += "\n"
    
    return result


def get_recent_logs(minutes: int, max_results: int = 100):
    """
    Get activity logs from the last N minutes.
    
    Args:
        minutes: Number of minutes to look back
        max_results: Maximum number of results to return
    
    Returns:
        str: Formatted results or error message
    """
    try:
        # Get log file paths
        log_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        readable_log_path = os.path.join(log_dir, 'activity_log_readable.csv')
        
        if not os.path.exists(readable_log_path):
            return "Activity log not found. No events have been logged yet."
        
        # Calculate cutoff time
        cutoff_time = datetime.now() - timedelta(minutes=minutes)
        
        # Read and filter logs
        matches = []
        with open(readable_log_path, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                timestamp_str = row.get('TIMESTAMP', '')
                if not timestamp_str:
                    continue
                
                try:
                    # Parse timestamp in DD/MM/YYYY-HH:MM:SS format
                    log_time = datetime.strptime(timestamp_str, '%d/%m/%Y-%H:%M:%S')
                    
                    if log_time >= cutoff_time:
                        matches.append(row)
                        
                        if len(matches) >= max_results:
                            break
                except ValueError:
                    # Skip rows with invalid timestamps
                    continue
        
        if not matches:
            return f"No activity found in the last {minutes} minute(s)."
        
        # Format results
        result = f"Activity from last {minutes} minute(s) ({len(matches)} events):\n"
        result += "=" * 50 + "\n\n"
        
        for i, match in enumerate(matches, 1):
            timestamp = match.get('TIMESTAMP', '')
            uid = match.get('UID', '')[:12] + '...' if match.get('UID', '') else 'N/A'
            event = match.get('EVENT', 'unknown event')
            
            result += f"{i}. [{timestamp}] {uid}\n"
            result += f"   {event}\n\n"
        
        if len(matches) == max_results:
            result += f"(Showing first {max_results} results)"
        
        return result
        
    except Exception as e:
        logger.error(f"Error retrieving recent logs: {e}")
        return f"Error retrieving logs: {str(e)}"


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
        clients_ref_path = os.path.join(log_dir, 'clients_reference.csv')
        
        if not os.path.exists(readable_log_path):
            return "Activity log not found. No events have been logged yet."
        
        # First, try to find UID from nickname or IP in both reference sources
        target_uids = set()
        matched_user_info = {}  # uid -> (nickname, ip)
        
        # Check clients_reference.csv first (more up-to-date)
        if os.path.exists(clients_ref_path):
            with open(clients_ref_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    uid = row.get('uid', '')
                    nickname = row.get('nickname', '')
                    ip = row.get('ip', '')
                    
                    # Check if search term matches UID, nickname, or IP (case-insensitive)
                    if (search_term.lower() in uid.lower() or
                        search_term.lower() in nickname.lower() or
                        search_term in ip):
                        target_uids.add(uid)
                        matched_user_info[uid] = (nickname, ip)
        
        # Also check users_seen.csv for historical data
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
                        if uid not in matched_user_info:
                            matched_user_info[uid] = (nickname, ip)
        
        if not target_uids:
            # Last resort: assume search_term is a UID directly
            target_uids.add(search_term)
            logger.debug(f"No match found in reference files, using search term as UID: {search_term}")
        
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
        
        result = f"Found {len(matches)} activities for '{user_display}':\n"
        for i, match in enumerate(matches, 1):
            timestamp = match.get('TIMESTAMP', '')
            event = match.get('EVENT', 'unknown event')
            
            result += f"{i}. [{timestamp}] {event}\n"
        
        if len(matches) == max_results:
            result += f"\n(Showing first {max_results} results)"
        
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
            "!lastminuteslogs <minutes> - Get activity from last N minutes\n"
            "!registerexp - Register for guild exp notifications\n"
            "!unregisterexp - Unregister from guild exp notifications\n"
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
        return format_snapshot(snapshot)
    
    # Search activity log
    if msg.startswith("!logger"):
        search_term = msg[7:].strip()
        if not search_term:
            return "Usage: !logger <uid/nickname/ip>"
        return search_activity_log(search_term)
    
    # Get recent logs by minutes
    if msg.startswith("!lastminuteslogs"):
        try:
            minutes_str = msg[16:].strip()
            if not minutes_str:
                return "Usage: !lastminuteslogs <minutes>\nExample: !lastminuteslogs 5"
            
            minutes = int(minutes_str)
            if minutes <= 0:
                return "Minutes must be a positive number."
            if minutes > 1440:  # 24 hours
                return "Maximum 1440 minutes (24 hours) allowed."
            
            return get_recent_logs(minutes)
        except ValueError:
            return "Invalid number. Usage: !lastminuteslogs <minutes>"
    
    # Register for guild exp notifications
    if msg.startswith("!registerexp"):
        # Get user UID from bot
        try:
            clid = bot.conn.clientlist().parsed
            user_uid = None
            for client in clid:
                if client.get('client_nickname') == nickname:
                    user_uid = client.get('client_unique_identifier', '')
                    break
            
            if user_uid:
                return register_exp_user(user_uid)
            else:
                return "Could not find your UID. Please try again."
        except Exception as e:
            logger.error(f"Error in registerexp command: {e}")
            return "Error registering. Please try again."
    
    # Unregister from guild exp notifications
    if msg.startswith("!unregisterexp"):
        # Get user UID from bot
        try:
            clid = bot.conn.clientlist().parsed
            user_uid = None
            for client in clid:
                if client.get('client_nickname') == nickname:
                    user_uid = client.get('client_unique_identifier', '')
                    break
            
            if user_uid:
                return unregister_exp_user(user_uid)
            else:
                return "Could not find your UID. Please try again."
        except Exception as e:
            logger.error(f"Error in unregisterexp command: {e}")
            return "Error unregistering. Please try again."
    
    # Unknown command




    t=get_txt()
    if isinstance(t, str) and t.strip():
        t=f"\n{t.strip()}"
    else:
        t=""
    default_response = f"esse nao é o x3tbot, digite !help para ver os comandos disponiveis {t}"
    
    return str(default_response)
