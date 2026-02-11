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
        logger.debug(f"Registering UID: {uid} in file: {registered_file}")
        # Load existing UIDs
        registered_uids = set()
        if os.path.exists(registered_file):
            with open(registered_file, 'r', encoding='utf-8') as f:
                logger.debug(f"Reading existing registered UIDs from {registered_file}")
                registered_uids = set(line.strip() for line in f if line.strip())
        
        # Check if already registered
        if uid in registered_uids:
            return "[color=#FFD700]You are already registered for guild exp notifications.[/color]"
        
        # Add new UID
        registered_uids.add(uid)
        
        # Write back to file
        with open(registered_file, 'w', encoding='utf-8') as f:
            for registered_uid in sorted(registered_uids):
                f.write(f"{registered_uid}\n")
        
        logger.info(f"Registered {uid} for guild exp notifications")
        return "[b][color=#00FF00]✅ Successfully registered for guild exp notifications![/color][/b]"
        
    except Exception as e:
        logger.error(f"Error registering user for exp notifications: {e}")
        return f"[color=#FF0000]Error registering: {str(e)}[/color]"


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
            return "[color=#FFD700]You are not registered for guild exp notifications.[/color]"
        
        # Load existing UIDs
        registered_uids = set()
        with open(registered_file, 'r', encoding='utf-8') as f:
            registered_uids = set(line.strip() for line in f if line.strip())
        
        # Check if registered
        if uid not in registered_uids:
            return "[color=#FFD700]You are not registered for guild exp notifications.[/color]"
        
        # Remove UID
        registered_uids.remove(uid)
        
        # Write back to file
        with open(registered_file, 'w', encoding='utf-8') as f:
            for registered_uid in sorted(registered_uids):
                f.write(f"{registered_uid}\n")
        
        logger.info(f"Unregistered {uid} from guild exp notifications")
        return "[b][color=#00FF00]✅ Successfully unregistered from guild exp notifications.[/color][/b]"
        
    except Exception as e:
        logger.error(f"Error unregistering user from exp notifications: {e}")
        return f"[color=#FF0000]Error unregistering: {str(e)}[/color]"


# COMMENTED OUT - Friendly guild exp functions not needed anymore
# def register_friendly_exp_user(uid: str) -> str:
#     """
#     Register a user for friendly guild exp notifications.
#     
#     Args:
#         uid: User UID to register
#     
#     Returns:
#         str: Success or error message
#     """
#     try:
#         log_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
#         registered_file = os.path.join(log_dir, 'registered_friendly.txt')
#         logger.debug(f"Registering UID: {uid} in file: {registered_file}")
#         # Load existing UIDs
#         registered_uids = set()
#         if os.path.exists(registered_file):
#             with open(registered_file, 'r', encoding='utf-8') as f:
#                 logger.debug(f"Reading existing registered UIDs from {registered_file}")
#                 registered_uids = set(line.strip() for line in f if line.strip())
#         
#         # Check if already registered
#         if uid in registered_uids:
#             return "You are already registered for friendly guild exp notifications."
#         
#         # Add new UID
#         registered_uids.add(uid)
#         
#         # Write back to file
#         with open(registered_file, 'w', encoding='utf-8') as f:
#             for registered_uid in sorted(registered_uids):
#                 f.write(f"{registered_uid}\n")
#         
#         logger.info(f"Registered {uid} for friendly guild exp notifications")
#         return "Successfully registered for friendly guild exp notifications!"
#         
#     except Exception as e:
#         logger.error(f"Error registering user for friendly exp notifications: {e}")
#         return f"Error registering: {str(e)}"
# 
# 
# def unregister_friendly_exp_user(uid: str) -> str:
#     """
#     Unregister a user from friendly guild exp notifications.
#     
#     Args:
#         uid: User UID to unregister
#     
#     Returns:
#         str: Success or error message
#     """
#     try:
#         log_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
#         registered_file = os.path.join(log_dir, 'registered_friendly.txt')
#         
#         if not os.path.exists(registered_file):
#             return "You are not registered for friendly guild exp notifications."
#         
#         # Load existing UIDs
#         registered_uids = set()
#         with open(registered_file, 'r', encoding='utf-8') as f:
#             registered_uids = set(line.strip() for line in f if line.strip())
#         
#         # Check if registered
#         if uid not in registered_uids:
#             return "You are not registered for friendly guild exp notifications."
#         
#         # Remove UID
#         registered_uids.remove(uid)
#         
#         # Write back to file
#         with open(registered_file, 'w', encoding='utf-8') as f:
#             for registered_uid in sorted(registered_uids):
#                 f.write(f"{registered_uid}\n")
#         
#         logger.info(f"Unregistered {uid} from friendly guild exp notifications")
#         return "Successfully unregistered from friendly guild exp notifications."
#         
#     except Exception as e:
#         logger.error(f"Error unregistering user from friendly exp notifications: {e}")
#         return f"Error unregistering: {str(e)}"


def format_war_stats(data, last_update):
    """
    Format war statistics data in human-readable format.
    
    Args:
        data: Dict with war stats from API
        last_update: Datetime of last update
    
    Returns:
        str: Formatted war stats
    """
    if not data:
        return "[color=#FF6B6B]No war statistics available yet. Please wait for the first data collection.[/color]"
    
    try:
        result = "[b][color=#FFD700]═══ WAR STATISTICS ═══[/color][/b]\n"
        if last_update:
            result += f"[color=#A0A0A0]Last Update: {last_update.strftime('%Y-%m-%d %H:%M:%S')}[/color]\n"
        result += "[color=#505050]" + "═" * 50 + "[/color]\n\n"
        
        for key in ["shell", "ascended"]:
            g = data.get(key, {})
            guild_color = "#4ECDC4" if key == "shell" else "#FF6B9D"
            
            result += f"[b][color={guild_color}]⚔️ Guild: {key.upper()}[/color][/b]\n"
            result += f"[color=#90EE90]● Online:[/color] [b]{g.get('totalOnline', 0)}[/b]\n"
            result += f"[color=#98D8C8]▲ Total Gained:[/color] [b]{g.get('totalGained', 0):,}[/b]\n"
            result += f"[color=#FF7F7F]▼ Total Lost:[/color] [b]{g.get('totalLost', 0):,}[/b]\n"
            
            net = g.get('totalGained', 0) - g.get('totalLost', 0)
            net_color = "#00FF00" if net > 0 else "#FF0000" if net < 0 else "#FFFFFF"
            result += f"[color=#FFD700]═ Net:[/color] [b][color={net_color}]{net:+,}[/color][/b]\n\n"
            
            membros = g.get("members", [])
            
            # Top 3 gains
            gainers = sorted([m for m in membros if m.get("delta", 0) > 0], 
                           key=lambda x: -x.get("delta", 0))[:3]
            if gainers:
                result += "[b][color=#00FF00]🏆 Top 3 Gains:[/color][/b]\n"
                for i, m in enumerate(gainers, 1):
                    result += f"  [color=#FFD700]{i}.[/color] [b]{m.get('name', 'Unknown')}[/b] - LV {m.get('level', '?')} - [color=#00FF00]Δ {m.get('delta', 0):+,}[/color]\n"
                result += "\n"
            
            # Top 3 losses
            losers = sorted([m for m in membros if m.get("delta", 0) < 0], 
                          key=lambda x: x.get("delta", 0))[:3]
            if losers:
                result += "[b][color=#FF6B6B]💀 Top 3 Losses:[/color][/b]\n"
                for i, m in enumerate(losers, 1):
                    result += f"  [color=#FFD700]{i}.[/color] [b]{m.get('name', 'Unknown')}[/b] - LV {m.get('level', '?')} - [color=#FF6B6B]Δ {m.get('delta', 0):+,}[/color]\n"
                result += "\n"
            
            result += "[color=#505050]" + "─" * 50 + "[/color]\n\n"
        
        return result
        
    except Exception as e:
        logger.error(f"Error formatting war stats: {e}")
        return f"[color=#FF0000]Error formatting war statistics: {str(e)}[/color]"


def get_txt():
    api_url="https://xinga-me.appspot.com/api"
    try:
        response=requests.get(api_url,verify=False)
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
        users_seen_path = os.path.join(log_dir, 'users_seen.csv')
        clients_ref_path = os.path.join(log_dir, 'clients_reference.csv')
        
        if not os.path.exists(readable_log_path):
            return "[color=#FF6B6B]Activity log not found. No events have been logged yet.[/color]"
        
        # Build UID to nickname mapping
        uid_to_nickname = {}
        
        # Read from clients_reference.csv (current data)
        if os.path.exists(clients_ref_path):
            with open(clients_ref_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    uid = row.get('uid', '').strip()
                    nickname = row.get('nickname', '').strip()
                    if uid and nickname:
                        uid_to_nickname[uid] = nickname
        
        # Read from users_seen.csv (historical data) - don't overwrite existing
        if os.path.exists(users_seen_path):
            with open(users_seen_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    uid = row.get('UID', '').strip()
                    nickname = row.get('NICKNAME', '').strip()
                    if uid and nickname and uid not in uid_to_nickname:
                        uid_to_nickname[uid] = nickname
        
        # Calculate cutoff time
        cutoff_time = datetime.now() - timedelta(minutes=minutes)
        
        # Read and filter logs
        matches = []
        with open(readable_log_path, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                timestamp_str = row.get('TIMESTAMP', '')
                uid = row.get('UID', '').strip()
                
                # Skip invalid entries
                if not timestamp_str or not uid or uid.upper() == 'N/A':
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
            return f"[color=#FF6B6B]No activity found in the last {minutes} minute(s).[/color]"
        
        # Format results
        result = f"[b][color=#4ECDC4]📋 Activity from last {minutes} minute(s)[/color][/b] [color=#A0A0A0]({len(matches)} events)[/color]\n"
        result += "[color=#505050]" + "═" * 50 + "[/color]\n\n"
        
        for i, match in enumerate(matches, 1):
            timestamp = match.get('TIMESTAMP', '')
            uid = match.get('UID', '').strip()
            event = match.get('EVENT', 'unknown event')
            
            # Get nickname from mapping, fallback to truncated UID
            nickname = uid_to_nickname.get(uid, uid[:12] + '...')
            
            result += f"[color=#FFD700]{i}.[/color] [color=#90EE90][{timestamp}][/color] [b]{nickname}[/b]\n"
            result += f"   [color=#FFFFFF]{event}[/color]\n\n"
        
        if len(matches) == max_results:
            result += f"[color=#A0A0A0](Showing first {max_results} results)[/color]"
        
        return result
        
    except Exception as e:
        logger.error(f"Error retrieving recent logs: {e}")
        return f"[color=#FF0000]Error retrieving logs: {str(e)}[/color]"


def get_registered_count():
    """
    Get the number of users registered for exp notifications.
    
    Returns:
        str: Count message or error
    """
    try:
        log_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        registered_file = os.path.join(log_dir, 'registered.txt')
        
        if not os.path.exists(registered_file):
            return "[color=#FF6B6B]📋 No users registered for exp notifications.[/color]"
        
        with open(registered_file, 'r', encoding='utf-8') as f:
            count = len([line for line in f if line.strip()])
        
        return f"[b][color=#4ECDC4]📋 Users registered for exp notifications:[/color][/b] [color=#FFD700]{count}[/color]"
    except Exception as e:
        logger.error(f"Error getting registered count: {e}")
        return f"[color=#FF0000]Error: {str(e)}[/color]"


def get_bot_uptime(bot):
    """
    Get bot uptime.
    
    Args:
        bot: TS3Bot instance
    
    Returns:
        str: Uptime information
    """
    if hasattr(bot, 'start_time'):
        uptime = datetime.now() - bot.start_time
        days = uptime.days
        hours, remainder = divmod(uptime.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"[b][color=#4ECDC4]⏱️ Bot uptime:[/color][/b] [color=#FFD700]{days}d {hours}h {minutes}m {seconds}s[/color]"
    return "[color=#FF6B6B]⏱️ Uptime information not available.[/color]"


def get_users_list():
    """
    Get list of all UIDs with their associated nicknames.
    
    Returns:
        str: Formatted list of UIDs and nicknames
    """
    try:
        log_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        users_seen_path = os.path.join(log_dir, 'users_seen.csv')
        clients_ref_path = os.path.join(log_dir, 'clients_reference.csv')
        
        # Dictionary to store uid -> set of nicknames
        uid_nicknames = {}
        
        # Read from users_seen.csv (historical data)
        if os.path.exists(users_seen_path):
            with open(users_seen_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    uid = row.get('UID', '').strip()
                    nickname = row.get('NICKNAME', '').strip()
                    
                    if uid and nickname:
                        if uid not in uid_nicknames:
                            uid_nicknames[uid] = set()
                        uid_nicknames[uid].add(nickname)
        
        # Read from clients_reference.csv (current data)
        if os.path.exists(clients_ref_path):
            with open(clients_ref_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    uid = row.get('uid', '').strip()
                    nickname = row.get('nickname', '').strip()
                    
                    if uid and nickname:
                        if uid not in uid_nicknames:
                            uid_nicknames[uid] = set()
                        uid_nicknames[uid].add(nickname)
        
        if not uid_nicknames:
            return "[color=#FF6B6B]No user data available.[/color]"
        
        # Format results
        result = f"[b][color=#4ECDC4]👥 Users List[/color][/b] [color=#A0A0A0]({len(uid_nicknames)} unique UIDs)[/color]\n"
        result += "[color=#505050]" + "═" * 50 + "[/color]\n\n"
        
        for uid, nicknames in sorted(uid_nicknames.items()):
            # Truncate UID for display
            uid_display = uid[:12] + '...' if len(uid) > 12 else uid
            nicks_list = ', '.join(sorted(nicknames))
            result += f"[color=#90EE90]{uid_display}[/color] [color=#FFD700]→[/color] [b]{nicks_list}[/b]\n"
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting users list: {e}")
        return f"[color=#FF0000]Error: {str(e)}[/color]"


def search_activity_log(search_term: str, max_results: int = 50):
    """
    Search human-readable activity log for entries matching uid, nickname, or ip.
    
    Args:
        search_term: The uid, nickname, or ip to search for
        max_results: Maximum number of results to return (default: 50, shows LAST entries)
    
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
            return "[color=#FF6B6B]Activity log not found. No events have been logged yet.[/color]"
        
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
        
        # Now search the readable activity log for these UIDs - collect ALL matches
        all_matches = []
        with open(readable_log_path, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                uid = row.get('UID', '')
                if uid in target_uids:
                    all_matches.append(row)
        
        if not all_matches:
            return f"[color=#FF6B6B]No activity found for: {search_term}[/color]"
        
        # Take only the LAST max_results entries
        matches = all_matches[-max_results:]
        total_found = len(all_matches)
        
        # Format results
        user_display = search_term
        if len(target_uids) == 1:
            uid = list(target_uids)[0]
            if uid in matched_user_info:
                nickname, ip = matched_user_info[uid]
                user_display = f"{nickname} ({uid[:8]}...)"
        
        result = f"[b][color=#4ECDC4]🔍 Found {total_found} activities for '[/color][color=#FFD700]{user_display}[/color][color=#4ECDC4]'[/color][/b] [color=#A0A0A0](showing last {len(matches)})[/color]\n\n"
        for i, match in enumerate(matches, 1):
            timestamp = match.get('TIMESTAMP', '')
            event = match.get('EVENT', 'unknown event')
            
            result += f"[color=#FFD700]{i}.[/color] [color=#90EE90][{timestamp}][/color] {event}\n"
        
        if total_found > max_results:
            result += f"\n[color=#A0A0A0](Showing last {max_results} of {total_found} total results)[/color]"
        
        return result
        
    except Exception as e:
        logger.error(f"Error searching activity log: {e}")
        return f"[color=#FF0000]Error searching log: {str(e)}[/color]"


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
            "[b][color=#FFD700]═════════════════════════════════════[/color][/b]\n"
            "[b][color=#4ECDC4]🤖 ROLLABOT - Available Commands[/color][/b]\n"
            "[b][color=#FFD700]═════════════════════════════════════[/color][/b]\n\n"
            "[b][color=#90EE90]!help[/color][/b] - Show this message\n"
            "[b][color=#90EE90]!mp[/color][/b] [color=#A0A0A0]<message>[/color] - Poke everyone (useful if x3tbot is offline)\n"
            "[b][color=#90EE90]!logger[/color][/b] [color=#A0A0A0]<uid/nickname/ip>[/color] - Search activity log for user\n"
            "[b][color=#90EE90]!lastminuteslogs[/color][/b] [color=#A0A0A0]<minutes>[/color] - Get activity from last N minutes\n"
            "[b][color=#90EE90]!users[/color][/b] - List all UIDs with their associated nicknames\n"
            "[b][color=#90EE90]!registered[/color][/b] - Show number of users registered for exp\n"
            "[b][color=#90EE90]!uptime[/color][/b] - Show bot uptime\n"
            "[b][color=#90EE90]!registerexp[/color][/b] - Register for guild exp notifications\n"
            "[b][color=#90EE90]!unregisterexp[/color][/b] - Unregister from guild exp notifications\n"
            "[b][color=#90EE90]!warexp[/color][/b] - Show war statistics (Shell vs Ascended)\n"
            "[color=#505050]─────────────────────────────────────[/color]"
            # "!registerfriendlyexp - Register for friendly guild exp notifications\n"  # Commented out
            # "!unregisterfriendlyexp - Unregister from friendly guild exp notifications\n"  # Commented out
        )





    # Mass poke command
    if msg.startswith("!mp"):
        bot.masspoke(f"{nickname} te cutucou: {msg[4:]}")
        return "[b][color=#4ECDC4]📢 Poking all clients...[/color][/b]"
    
    # Add to hunted list (via x3tBot)#hide from help since it's x3tBot specific
    #if msg.startswith("!hunted add"):
    #    target = msg[12:].strip()
    #    return bot.add_hunted(target)
    #
    ## Get detailed client snapshot
    #if msg.startswith("!snapshot"):
    #    snapshot = bot.conn.clientlist(
    #        info=True, country=True, uid=True, ip=True,
    #        groups=True, times=True, voice=True, away=True
    #    ).parsed
    #    return format_snapshot(snapshot)
    
    # Search activity log
    if msg.startswith("!logger"):
        search_term = msg[7:].strip()
        if not search_term:
            return "[color=#FF6B6B]Usage:[/color] [b]!logger[/b] [color=#A0A0A0]<uid/nickname/ip>[/color]"
        return search_activity_log(search_term)
    
    # Get recent logs by minutes
    if msg.startswith("!lastminuteslogs"):
        try:
            minutes_str = msg[16:].strip()
            if not minutes_str:
                return "[color=#FF6B6B]Usage:[/color] [b]!lastminuteslogs[/b] [color=#A0A0A0]<minutes>[/color]\n[color=#90EE90]Example:[/color] !lastminuteslogs 5"
            
            minutes = int(minutes_str)
            if minutes <= 0:
                return "[color=#FF6B6B]Minutes must be a positive number.[/color]"
            if minutes > 1440:  # 24 hours
                return "[color=#FF6B6B]Maximum 1440 minutes (24 hours) allowed.[/color]"
            
            return get_recent_logs(minutes)
        except ValueError:
            return "[color=#FF6B6B]Invalid number. Usage:[/color] [b]!lastminuteslogs[/b] [color=#A0A0A0]<minutes>[/color]"
    
    # Get list of all users with their nicknames
    if msg.startswith("!users"):
        return get_users_list()
    
    # Get registered users count
    if msg.startswith("!registered"):
        return get_registered_count()
    
    # Get bot uptime
    if msg.startswith("!uptime"):
        return get_bot_uptime(bot)
    
    # Register for guild exp notifications
    if msg.startswith("!registerexp"):
        # Get user UID from reference data (avoid API calls from event loop)
        try:
            logger.debug(f"Processing registerexp for nickname: {nickname}")
            user_uid = None
            
            # Use reference manager's client_map if available
            if hasattr(bot, 'client_map') and bot.client_map:
                logger.debug("Looking up UID in bot's client_map")
                for clid, client_info in bot.client_map.items():
                    logger.debug(f"Checking client: {client_info.get('nickname', '')}")
                    if client_info.get('nickname', '').lower() == nickname.lower():
                        logger.debug(f"Found matching client: {client_info}")
                        user_uid = client_info.get('uid', '')
                        logger.debug(f"Extracted UID: {user_uid}")
                        break
            
            # Fallback: Read from CSV if not in memory

           
            if not user_uid:
                logger.debug("Looking up UID in clients_reference.csv")
                try:
                    log_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                    clients_ref_path = os.path.join(log_dir, 'clients_reference.csv')
                    logger.debug(f"Checking for clients_reference.csv at: {clients_ref_path}")
                    if os.path.exists(clients_ref_path):
                        logger.debug("Found clients_reference.csv, reading file")
                        with open(clients_ref_path, 'r', newline='', encoding='utf-8') as f:
                            reader = csv.DictReader(f)
                            for row in reader:
                                if row.get('nickname', '').lower() == nickname.lower():
                                    user_uid = row.get('uid', '')
                                    break
                except Exception as ref_error:
                    logger.debug(f"Could not read reference data: {ref_error}")
            
            if user_uid:
                logger.debug(f"Registering user UID: {user_uid} for exp notifications")
                return register_exp_user(user_uid)
            else:
                return "[color=#FF6B6B]Could not find your UID. Please wait a minute for data to refresh and try again.[/color]"
        except Exception as e:
            logger.error(f"Error in registerexp command: {e}")
            return "[color=#FF0000]Error registering. Please try again.[/color]"
    
    # Unregister from guild exp notifications
    if msg.startswith("!unregisterexp"):
        # Get user UID from reference data (avoid API calls from event loop)
        try:
            user_uid = None
            
            # Use reference manager's client_map if available
            if hasattr(bot, 'client_map') and bot.client_map:
                for clid, client_info in bot.client_map.items():
                    if client_info.get('nickname', '').lower() == nickname.lower():
                        user_uid = client_info.get('uid', '')
                        break
            
            # Fallback: Read from CSV if not in memory
            if not user_uid:
                try:
                    log_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                    clients_ref_path = os.path.join(log_dir, 'clients_reference.csv')
                    
                    if os.path.exists(clients_ref_path):
                        with open(clients_ref_path, 'r', newline='', encoding='utf-8') as f:
                            reader = csv.DictReader(f)
                            for row in reader:
                                if row.get('nickname', '').lower() == nickname.lower():
                                    user_uid = row.get('uid', '')
                                    break
                except Exception as ref_error:
                    logger.debug(f"Could not read reference data: {ref_error}")
            
            if user_uid:
                return unregister_exp_user(user_uid)
            else:
                return "[color=#FF6B6B]Could not find your UID. Please wait a minute for data to refresh and try again.[/color]"
        except Exception as e:
            logger.error(f"Error in unregisterexp command: {e}")
            return "[color=#FF0000]Error unregistering. Please try again.[/color]"
    
    # COMMENTED OUT - Friendly guild exp commands not needed anymore
    # # Register for friendly guild exp notifications
    # if msg.startswith("!registerfriendlyexp"):
    #     # Get user UID from reference data (avoid API calls from event loop)
    #     try:
    #         logger.debug(f"Processing registerfriendlyexp for nickname: {nickname}")
    #         user_uid = None
    #         
    #         # Use reference manager's client_map if available
    #         if hasattr(bot, 'client_map') and bot.client_map:
    #             logger.debug("Looking up UID in bot's client_map")
    #             for clid, client_info in bot.client_map.items():
    #                 logger.debug(f"Checking client: {client_info.get('nickname', '')}")
    #                 if client_info.get('nickname', '').lower() == nickname.lower():
    #                     logger.debug(f"Found matching client: {client_info}")
    #                     user_uid = client_info.get('uid', '')
    #                     logger.debug(f"Extracted UID: {user_uid}")
    #                     break
    #         
    #         # Fallback: Read from CSV if not in memory
    #         if not user_uid:
    #             logger.debug("Looking up UID in clients_reference.csv")
    #             try:
    #                 log_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    #                 clients_ref_path = os.path.join(log_dir, 'clients_reference.csv')
    #                 logger.debug(f"Checking for clients_reference.csv at: {clients_ref_path}")
    #                 if os.path.exists(clients_ref_path):
    #                     logger.debug("Found clients_reference.csv, reading file")
    #                     with open(clients_ref_path, 'r', newline='', encoding='utf-8') as f:
    #                         reader = csv.DictReader(f)
    #                         for row in reader:
    #                             if row.get('nickname', '').lower() == nickname.lower():
    #                                 user_uid = row.get('uid', '')
    #                                 break
    #             except Exception as ref_error:
    #                 logger.debug(f"Could not read reference data: {ref_error}")
    #         
    #         if user_uid:
    #             logger.debug(f"Registering user UID: {user_uid} for friendly exp notifications")
    #             return register_friendly_exp_user(user_uid)
    #         else:
    #             return "Could not find your UID. Please wait a minute for data to refresh and try again."
    #     except Exception as e:
    #         logger.error(f"Error in registerfriendlyexp command: {e}")
    #         return "Error registering. Please try again."
    # 
    # # Unregister from friendly guild exp notifications
    # if msg.startswith("!unregisterfriendlyexp"):
    #     # Get user UID from reference data (avoid API calls from event loop)
    #     try:
    #         user_uid = None
    #         
    #         # Use reference manager's client_map if available
    #         if hasattr(bot, 'client_map') and bot.client_map:
    #             for clid, client_info in bot.client_map.items():
    #                 if client_info.get('nickname', '').lower() == nickname.lower():
    #                     user_uid = client_info.get('uid', '')
    #                     break
    #         
    #         # Fallback: Read from CSV if not in memory
    #         if not user_uid:
    #             try:
    #                 log_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    #                 clients_ref_path = os.path.join(log_dir, 'clients_reference.csv')
    #                 
    #                 if os.path.exists(clients_ref_path):
    #                     with open(clients_ref_path, 'r', newline='', encoding='utf-8') as f:
    #                         reader = csv.DictReader(f)
    #                         for row in reader:
    #                             if row.get('nickname', '').lower() == nickname.lower():
    #                                 user_uid = row.get('uid', '')
    #                                 break
    #             except Exception as ref_error:
    #                 logger.debug(f"Could not read reference data: {ref_error}")
    #         
    #         if user_uid:
    #             return unregister_friendly_exp_user(user_uid)
    #         else:
    #             return "Could not find your UID. Please wait a minute for data to refresh and try again."
    #     except Exception as e:
    #         logger.error(f"Error in unregisterfriendlyexp command: {e}")
    #         return "Error unregistering. Please try again."
    
    # War statistics command
    if msg.startswith("!warexp"):
        try:
            if not hasattr(bot, 'war_stats_collector'):
                return "[color=#FF6B6B]War statistics collector is not available.[/color]"
            
            stats_data, last_update = bot.war_stats_collector.get_stats()
            return format_war_stats(stats_data, last_update)
        except Exception as e:
            logger.error(f"Error in warexp command: {e}")
            return "[color=#FF0000]Error retrieving war statistics. Please try again.[/color]"
    
    # Unknown command




    t=get_txt()
    if isinstance(t, str) and t.strip():
        t=f"\n[color=#FF6B6B]{t.strip()}[/color]"
    else:
        t=""
    default_response = f"[color=#A0A0A0]Esse não é o x3tbot. Digite[/color] [b][color=#4ECDC4]!help[/color][/b] [color=#A0A0A0]para ver os comandos disponíveis[/color] {t}"
    
    return str(default_response)
