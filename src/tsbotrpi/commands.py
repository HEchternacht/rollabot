"""
Command handlers for the TS3 bot.
Edit this file to add/modify bot commands.
"""
import logging
import csv
import os
import time
import threading
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

import requests
import json

def get_war_exp_log(days: int = 30) -> str:
    """
    Get war exp log for the last N days.
    
    Args:
        days: Number of days to retrieve (default: 30)
    
    Returns:
        str: Formatted war exp log
    """
    try:
        log_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        exps_file = os.path.join(log_dir, 'exps.csv')
        
        if not os.path.exists(exps_file):
            return "[color=#FF6B6B]No war exp log found.[/color]"
        
        # Read all rows
        rows = []
        with open(exps_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)
        
        if not rows:
            return "[color=#FF6B6B]No war exp data available.[/color]"
        
        # Filter by date range - get last N days including today
        from datetime import datetime, timedelta
        today = datetime.now()
        cutoff_date = today - timedelta(days=days - 1)  # -1 to include today
        
        filtered_rows = []
        for row in rows:
            try:
                row_date = datetime.strptime(row.get('date', ''), '%d/%m/%Y')
                if row_date >= cutoff_date:
                    filtered_rows.append(row)
            except (ValueError, TypeError):
                # If date parsing fails, include the row anyway
                filtered_rows.append(row)
        
        # If no rows match the date filter, fall back to last N rows
        rows = filtered_rows if filtered_rows else rows[-days:]
        
        # Format output with new style
        message = f"[b][color=#FFD700]═══ War Exp Log (Last {days} Days) ═══[/color][/b]\n"
        message += "[color=#505050]" + "═" * 60 + "[/color]\n\n"
        
        for row in rows:
            date = row.get('date', 'Unknown')
            asc_exp = int(row.get('ascendant_exp', 0))
            shell_exp = int(row.get('shellpatrocina_exp', 0))
            asc_score = int(row.get('score_ascendant', 0))
            shell_score = int(row.get('score_shellpatrocina', 0))
            
            # Format: day > Ascendant <score> <exp> X <exp> <score> ShellPatrocina
            message += f"[color=#A0A0A0]{date}[/color] [color=#505050]>[/color] "
            message += f"[b][color=#FF6B9D]Ascendant[/color][/b] "
            message += f"[color=#00FF00]{asc_score}[/color] [color=#FFD700]{asc_exp:,}[/color] "
            message += f"[b][color=#FFFFFF]X[/color][/b] "
            message += f"[color=#FFD700]{shell_exp:,}[/color] [color=#00FF00]{shell_score}[/color] "
            message += f"[b][color=#4ECDC4]ShellPatrocina[/color][/b]\n"
        
        message += "\n[color=#505050]" + "═" * 60 + "[/color]"
        return message
    except Exception as e:
        logger.error(f"Error reading war exp log: {e}", exc_info=True)
        return "[color=#FF0000]Error reading war exp log.[/color]"


def get_exp_log(minutes: int = None, entries: int = 100) -> str:
    """
    Get exp deltas for the last N minutes or last N entries.
    
    Args:
        minutes: Number of minutes to retrieve (optional)
        entries: Number of entries to retrieve if minutes not specified (default: 100)
    
    Returns:
        str: Formatted exp deltas log
    """
    try:
        log_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        exp_deltas_file = os.path.join(log_dir, 'exp_deltas.csv')
        
        if not os.path.exists(exp_deltas_file):
            return "[color=#FF6B6B]No exp deltas log found.[/color]"
        
        # Read all rows
        all_rows = []
        with open(exp_deltas_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                all_rows.append(row)
        
        if not all_rows:
            return "[color=#FF6B6B]No exp deltas available.[/color]"
        
        # Filter by time or get last N entries
        rows = []
        if minutes is not None:
            cutoff_time = datetime.now() - timedelta(minutes=minutes)
            for row in all_rows:
                try:
                    timedate_str = row.get('timedate', '')
                    row_time = datetime.strptime(timedate_str, '%d/%m/%Y %H:%M')
                    if row_time >= cutoff_time:
                        rows.append(row)
                except ValueError:
                    continue
            
            if not rows:
                return f"[color=#FF6B6B]No exp deltas in the last {minutes} minutes.[/color]"
            
            header = f"[b][color=#FFD700]═══ Exp Deltas (Last {minutes} Minutes) ═══[/color][/b]"
        else:
            # Get last N entries
            rows = all_rows[-entries:] if entries < len(all_rows) else all_rows
            header = f"[b][color=#FFD700]═══ Exp Deltas (Last {len(rows)} Entries) ═══[/color][/b]"
        
        # Format output - simple list format
        message = header + "\n"
        message += "[color=#505050]" + "═" * 60 + "[/color]\n\n"
        
        # Show in reverse order (newest first)
        for row in reversed(rows):
            timedate = row.get('timedate', 'Unknown')
            name = row.get('name', 'Unknown')
            exp = row.get('exp', '0')
            
            # Format: <daytime> <name> <deltexp>
            message += f"[color=#A0A0A0]{timedate}[/color] [color=#4ECDC4]{name}[/color] [color=#00FF00]{exp}[/color]\n"
        
        message += "\n[color=#505050]" + "═" * 60 + "[/color]"
        return message
    except Exception as e:
        logger.error(f"Error reading exp deltas log: {e}", exc_info=True)
        return "[color=#FF0000]Error reading exp deltas log.[/color]"


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
        shell_members = data.get("shell", {}).get("members", [])
        ascended_members = data.get("ascended", {}).get("members", [])
        shell_deaths = len([m for m in shell_members if m.get("delta", 0) < 0])
        ascended_deaths = len([m for m in ascended_members if m.get("delta", 0) < 0])

        result = "[b][color=#FFD700]═══ WAR STATISTICS ═══[/color][/b]\n"
        result += (
            f"[b][color=#FF6B9D]Ascendant[/color] "
            f"[color=#FFD700]{shell_deaths}[/color] x "
            f"[color=#FFD700]{ascended_deaths}[/color] "
            f"[color=#4ECDC4]ShellPatrocina[/color][/b]\n"
            "\n"
            "[i]*Esse score não é de kills, mas sim de membros que perderam exp (delta negativo) do time oposto*[/i]\n"
        )
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


def get_users_list(plus_mode=False):
    """
    Get list of all UIDs with their associated nicknames.
    
    Args:
        plus_mode: If True, only show users with multiple nicknames
    
    Returns:
        str: Formatted list of UIDs and nicknames
    """
    try:
        log_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        uid_nicknames_path = os.path.join(log_dir, 'uid_nicknames.csv')
        
        # Dictionary to store uid -> set of nicknames
        uid_nicknames = {}
        
        # Read from uid_nicknames.csv
        if os.path.exists(uid_nicknames_path):
            with open(uid_nicknames_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    uid = row.get('UID', '').strip()
                    nickname = row.get('NICKNAME', '').strip()
                    
                    if uid and nickname:
                        if uid not in uid_nicknames:
                            uid_nicknames[uid] = set()
                        uid_nicknames[uid].add(nickname)
        
        if not uid_nicknames:
            return "[color=#FF6B6B]No user data available.[/color]"
        
        # Filter for plus mode (only users with >1 nickname)
        if plus_mode:
            uid_nicknames = {uid: nicks for uid, nicks in uid_nicknames.items() if len(nicks) > 1}
            
            if not uid_nicknames:
                return "[color=#FF6B6B]No users with multiple nicknames found.[/color]"
        
        # Format results
        mode_text = " Plus" if plus_mode else ""
        result = f"[b][color=#4ECDC4]👥 Users List{mode_text}[/color][/b] [color=#A0A0A0]({len(uid_nicknames)} unique UIDs)[/color]\n"
        result += "[color=#505050]" + "═" * 50 + "[/color]\n\n"
        
        for uid, nicknames in sorted(uid_nicknames.items()):
            # Truncate UID for display
            uid_display = uid[:12] + '...' if len(uid) > 12 else uid
            nicks_list = ', '.join(sorted(nicknames))
            
            # Add nickname count if more than 1
            if len(nicknames) > 1:
                result += f"[color=#90EE90]{uid_display}[/color] [color=#FFD700]→[/color] [b]{nicks_list}[/b] [color=#FF69B4]({len(nicknames)} names)[/color]\n"
            else:
                result += f"[color=#90EE90]{uid_display}[/color] [color=#FFD700]→[/color] [b]{nicks_list}[/b]\n"
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting users list: {e}")
        return f"[color=#FF0000]Error: {str(e)}[/color]"


def get_channel_list():
    """
    Get list of all channels with their IDs.
    
    Returns:
        str: Formatted list of channel IDs and names
    """
    try:
        log_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        channels_ref_path = os.path.join(log_dir, 'channels_reference.csv')
        
        # Dictionary to store cid -> channel name
        channels = {}
        
        # Read from channels_reference.csv
        if os.path.exists(channels_ref_path):
            with open(channels_ref_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    cid = row.get('cid', '').strip()
                    channel_name = row.get('channel_name', '').strip()
                    
                    if cid and channel_name:
                        channels[cid] = channel_name
        
        if not channels:
            return "[color=#FF6B6B]No channel data available.[/color]"
        
        # Format results
        result = f"[b][color=#4ECDC4]📋 Channels List[/color][/b] [color=#A0A0A0]({len(channels)} channels)[/color]\\n"
        result += "[color=#505050]" + "═" * 50 + "[/color]\\n\\n"
        
        for cid, channel_name in sorted(channels.items(), key=lambda x: int(x[0]) if x[0].isdigit() else 0):
            result += f"[color=#90EE90]{cid}[/color] [color=#FFD700]→[/color] [b]{channel_name}[/b]\\n"
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting channel list: {e}")
        return f"[color=#FF0000]Error: {str(e)}[/color]"


def periodic_kick_channel(bot, channel_id: str, duration_minutes: int, thread_id: str):
    """
    Monitor a channel and kick anyone who enters for a specified duration.
    
    Args:
        bot: TS3Bot instance
        channel_id: Channel ID to monitor and kick users from
        duration_minutes: Duration in minutes to monitor the channel
        thread_id: Unique thread identifier
    """
    try:
        end_time = time.time() + (duration_minutes * 60)
        check_interval = 10  # Check every 10 seconds
        kicked_uids = set()  # Track kicked users to show unique count
        
        logger.info(f"PKC {thread_id}: Starting channel {channel_id} monitoring for {duration_minutes} minutes")
        
        # Initial kick of everyone in the channel (reason will be auto-formatted with time remaining)
        result = bot.kick_channel_users(channel_id)
        if result['success']:
            logger.info(f"PKC {thread_id}: Initial kick - {result['kicked_count']} users removed from channel {channel_id}")
        
        # Monitor loop
        while time.time() < end_time:
            time.sleep(check_interval)
            
            # Check if still running (in case bot is shutting down)
            if not bot._running:
                break
            
            # Check for users in the channel and kick them (reason will be auto-formatted with time remaining)
            try:
                result = bot.kick_channel_users(channel_id)
                if result['success'] and result['kicked_count'] > 0:
                    logger.info(f"PKC {thread_id}: Kicked {result['kicked_count']} users from channel {channel_id}")
            except Exception as e:
                logger.warning(f"PKC {thread_id}: Error during check kick: {e}")
        
        remaining_time = max(0, int((end_time - time.time()) / 60))
        if remaining_time == 0:
            logger.info(f"PKC {thread_id}: Channel {channel_id} monitoring completed after {duration_minutes} minutes")
        else:
            logger.info(f"PKC {thread_id}: Channel {channel_id} monitoring stopped early")
        
    except Exception as e:
        logger.error(f"PKC {thread_id}: Error in channel monitoring thread: {e}", exc_info=True)
    
    finally:
        # Remove channel from active monitoring
        with bot.pkc_lock:
            if channel_id in bot.active_pkc_channels:
                del bot.active_pkc_channels[channel_id]
            logger.debug(f"PKC {thread_id}: Channel {channel_id} removed from active monitoring")


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
            return f"\n[color=#FF6B6B]No activity found for: {search_term}[/color]"
        
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
        return f"\n[color=#FF0000]Error searching log: {str(e)}[/color]"




def bdsm_text_async(nickname, bot, clid):
    """Call API in background and queue the response."""
    import threading
    
    def _fetch_and_send():
        #set_key="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"
        set_key = os.getenv("OPENROUTER_API_KEY", "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")
        
        try:
            response = requests.post(
                url="https://openrouter.ai/api/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {set_key}",
                    "Content-Type": "application/json",
                },
                data=json.dumps({
                    "model": "liquid/lfm-2.5-1.2b-instruct:free",
                    "messages": [
                        {
                            "role": "user",
                            "content": f"escreva uma historia e poetico de romance e erotismo com MUITA sexualização entre voce e um usuario que quer você romanticamente, o nome desse usuario é {nickname}, durante a historia crie apelidos caridosos para ele, derivaçoes do nome dele, crie momentos de tensão e use palavras chulas como 'Foder', 'Socar', 'Meter' e outras de conotação sexual:, de forma erotica,sem pre-amble ou post-amble, apenas a hsitoria raw escreva a historia para o usuario, entao se refira a pessoa como 'Você, {nickname}' por exemplo, 'Você, {nickname}, é tão gostoso que eu quero te foder' ou 'Eu quero meter meu pau em você, {nickname}' ou 'Eu quero socar minha buceta em você, e variaçoes, termine com a frase, 'Eu.... Te amo {nickname}, me da seu cuzinho?"
                        }
                    ]
                }),
                timeout=30
            )
            
            message = f"\n[b][color=#eeb0bb]{response.json()['choices'][0]['message']['content']}[/color][/b]"
        except Exception as e:
            logger.error(f"Error in bdsm API call: {e}")
            t = get_txt()
            message = f"\n[b][color=#eeb0bb]Vem ca seu {t if t is not None else 'Gostoso'}[/color][/b]"
        
        # Queue the message to be sent
        bot.command_queue.put({
            'type': 'send_message',
            'clid': clid,
            'message': message
        })
    
    # Start the thread
    thread = threading.Thread(target=_fetch_and_send, daemon=True)
    thread.start()





















def process_command(bot, msg, nickname, clid=None):
    """
    Process incoming messages and return response.
    
    Args:
        bot: TS3Bot instance (has methods like masspoke, add_hunted, etc.)
        msg: Message text from user
        nickname: Nickname of user who sent message
        clid: Client ID of user who sent message (optional)
    
    Returns:
        str: Response to send back to user
    """
    try:
        if msg.startswith("!help"):
            return (
                "\n"
                "[b][color=#FF1493]═════════════════════════════════════[/color][/b]\n"
                "[b][color=#FF69B4]🪲 ROLLABOT - Available Commands[/color][/b]\n"
                "[b][color=#FF1493]═════════════════════════════════════[/color][/b]\n\n"
                "[b][color=#FF4500]!help[/color][/b] - Show this message\n"
                "[b][color=#FF8C00]!mp[/color][/b] [color=#A0A0A0]<message>[/color] - Poke everyone (useful if x3tbot is offline)\n"
                "[b][color=#FFD700]!logger[/color][/b] [color=#A0A0A0]<uid/nickname/ip>[/color] - Search activity log for user\n"
                "[b][color=#9ACD32]!lastminuteslogs[/color][/b] [color=#A0A0A0]<minutes>[/color] - Get activity from last N minutes\n"
                "[b][color=#32CD32]!users[/color][/b] - List all UIDs with their associated nicknames\n"
                "[b][color=#228B22]!users plus[/color][/b] - List only users with multiple nicknames\n"
                "[b][color=#20B2AA]!channelids[/color][/b] - List all channels with their IDs\n"
                "[b][color=#00CED1]!registered[/color][/b] - Show number of users registered for exp\n"
                "[b][color=#1E90FF]!uptime[/color][/b] - Show bot uptime\n"
                "[b][color=#4169E1]!registerexp[/color][/b] - Register for guild exp notifications\n"
                "[b][color=#8A2BE2]!unregisterexp[/color][/b] - Unregister from guild exp notifications\n"
                "[b][color=#9932CC]!warexp[/color][/b] - Show war statistics (Shell vs Ascended)\n"
                "[b][color=#FF1493]!warexplog [days][/color][/b] - Show war exp history (default: 30 days)\n"
                "[b][color=#FF4500]!explog [minutes][/color][/b] - Show recent exp gains (default: 100 entries)\n"
                "[b][color=#FF8C00]!showlogs[/color][/b] - Show last 100 warnings/errors\n"
                "[b][color=#FFD700]!bdsm[/color][/b] - Move you and the bot to Djinns channel\n"
                "[b][color=#DC143C]!pkc[/color][/b] [color=#A0A0A0]<channel> <minutes>[/color] - Lock channel for duration (max 3 active)\n"
                "\n"
                "[i]Note: [color=#A0A0A0]Obrigado Pedrin pelas apis que eu robei na cara dura.[/color][/i]\n"
                "[color=#8B8B8B]─────────────────────────────────────[/color]"

                # "!registerfriendlyexp - Register for friendly guild exp notifications\n"  # Commented out
                # "!unregisterfriendlyexp - Unregister from friendly guild exp notifications\n"  # Commented out
            )





        # Mass poke command
        if msg.startswith("!mp"):
            bot.masspoke(f"{nickname} te cutucou: {msg[4:]}")
            return "\n[b][color=#4ECDC4]📢 Poking all clients...[/color][/b]"
        
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
                return "\n[color=#FF6B6B]Usage:[/color] [b]!logger[/b] [color=#A0A0A0]<uid/nickname/ip>[/color]"
            return "\n" + search_activity_log(search_term)
        
        # Get recent logs by minutes
        if msg.startswith("!lastminuteslogs"):
            try:
                minutes_str = msg[16:].strip()
                if not minutes_str:
                    return "\n[color=#FF6B6B]Usage:[/color] [b]!lastminuteslogs[/b] [color=#A0A0A0]<minutes>[/color]\n[color=#90EE90]Example:[/color] !lastminuteslogs 5"
                
                minutes = int(minutes_str)
                if minutes <= 0:
                    return "\n[color=#FF6B6B]Minutes must be a positive number.[/color]"
                if minutes > 1440:  # 24 hours
                    return "\n[color=#FF6B6B]Maximum 1440 minutes (24 hours) allowed.[/color]"
                
                return "\n" + get_recent_logs(minutes)
            except ValueError:
                return "\n[color=#FF6B6B]Invalid number. Usage:[/color] [b]!lastminuteslogs[/b] [color=#A0A0A0]<minutes>[/color]"
        
        # Get list of all users with their nicknames
        if msg.startswith("!users"):
            # Check for "plus" mode
            if "plus" in msg.lower():
                return "\n" + get_users_list(plus_mode=True)
            else:
                return "\n" + get_users_list(plus_mode=False)
        
        # Get list of all channels with their IDs
        if msg.startswith("!channelids"):
            return "\n" + get_channel_list()
        
        # Periodic kick channel command
        if msg.startswith("!pkc"):
            try:
                # Parse command: !pkc <channel_id> <duration_minutes>
                parts = msg.split()
                
                if len(parts) < 3:
                    return "\n[color=#FF6B6B]Usage:[/color] [b]!pkc[/b] [color=#A0A0A0]<channel_id> <duration_minutes>[/color]\n[color=#90EE90]Example:[/color] !pkc 5 30 (lock channel 5 for 30 minutes)"
                
                channel_id = parts[1]
                duration_minutes = int(parts[2])
                
                # Validate parameters
                if duration_minutes < 1:
                    return "\n[color=#FF6B6B]Duration must be at least 1 minute.[/color]"
                
                if duration_minutes > 180:
                    return "\n[color=#FF6B6B]Maximum duration is 180 minutes (3 hours).[/color]"
                
                # Check if max concurrent channels reached
                with bot.pkc_lock:
                    if len(bot.active_pkc_channels) >= 3:
                        return "\n[color=#FF6B6B]Max monitored channels active (3/3). Please wait for one to complete.[/color]"
                    
                    # Check if channel already being monitored
                    if channel_id in bot.active_pkc_channels:
                        return f"\n[color=#FF6B6B]Channel {channel_id} is already being monitored.[/color]"
                    
                    # Create thread info
                    thread_id = f"pkc_{channel_id}_{int(time.time())}"
                    thread = threading.Thread(
                        target=periodic_kick_channel,
                        args=(bot, channel_id, duration_minutes, thread_id),
                        daemon=True
                    )
                    
                    # Add to active channels
                    end_time = time.time() + (duration_minutes * 60)
                    bot.active_pkc_channels[channel_id] = {
                        'thread_id': thread_id,
                        'thread': thread,
                        'end_time': end_time,
                        'duration_minutes': duration_minutes,
                        'started': datetime.now()
                    }
                    
                    # Start thread
                    thread.start()
                
                return f"\n[b][color=#4ECDC4]🔒 Channel {channel_id} locked for {duration_minutes} minutes.[/color][/b]\n[color=#A0A0A0]Active monitors: {len(bot.active_pkc_channels)}/3[/color]"
                    
            except ValueError:
                return "\n[color=#FF6B6B]Invalid parameters. Usage:[/color] [b]!pkc[/b] [color=#A0A0A0]<channel_id> <duration_minutes>[/color]"
            except Exception as e:
                logger.error(f"Error in !pkc command: {e}")
                return f"\n[color=#FF0000]Error: {str(e)}[/color]"
        
        # Get registered users count
        if msg.startswith("!registered"):
            return "\n" + get_registered_count()
        
        # Get bot uptime
        if msg.startswith("!uptime"):
            return "\n" + get_bot_uptime(bot)
        
        # Get command history (hidden from !help)
        if msg.startswith("!history"):
            try:
                history = list(bot.command_history)
                if not history:
                    return "\n[color=#FF6B6B]No command history available.[/color]"
                
                # Reverse to show most recent first
                history.reverse()
                
                message = "[b][color=#FFD700]═══ Command History ═══[/color][/b]\n"
                message += "[color=#505050]" + "═" * 60 + "[/color]\n\n"
                
                for timestamp, nickname, command in history:
                    message += f"[color=#A0A0A0]{timestamp}[/color] [color=#505050]-[/color] "
                    message += f"[color=#4ECDC4]{nickname}[/color] [color=#505050]-[/color] "
                    message += f"[color=#FFD700]{command}[/color]\n"
                
                message += "\n[color=#505050]" + "═" * 60 + "[/color]"
                return "\n" + message
            except Exception as e:
                logger.error(f"Error retrieving command history: {e}")
                return "\n[color=#FF0000]Error retrieving command history.[/color]"
        
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
                    return "\n" + register_exp_user(user_uid)
                else:
                    return "\n[color=#FF6B6B]Could not find your UID. Please wait a minute for data to refresh and try again.[/color]"
            except Exception as e:
                logger.error(f"Error in registerexp command: {e}")
                return "\n[color=#FF0000]Error registering. Please try again.[/color]"
        
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
                    return "\n" + unregister_exp_user(user_uid)
                else:
                    return "\n[color=#FF6B6B]Could not find your UID. Please wait a minute for data to refresh and try again.[/color]"
            except Exception as e:
                logger.error(f"Error in unregisterexp command: {e}")
                return "\n[color=#FF0000]Error unregistering. Please try again.[/color]"
        
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

        # War exp log command
        if msg.startswith("!warexplog"):
            try:
                parts = msg.split()
                
                # Default to 30 days if no parameter
                if len(parts) < 2:
                    days = 30
                else:
                    days = int(parts[1])
                    if days < 1 or days > 365:
                        return "\n[color=#FF6B6B]Days must be between 1 and 365.[/color]"
                
                return "\n" + get_war_exp_log(days)
            except ValueError:
                return "\n[color=#FF6B6B]Invalid number of days. Usage: !warexplog [days][/color]"
            except Exception as e:
                logger.error(f"Error in warexplog command: {e}")
                return "\n[color=#FF0000]Error retrieving war exp log.[/color]"
        
        if msg.startswith("!warexp"):
            try:
                if not hasattr(bot, 'war_stats_collector'):
                    return "\n[color=#FF6B6B]War statistics collector is not available.[/color]"
                
                stats_data, last_update = bot.war_stats_collector.get_stats()
                return "\n" + format_war_stats(stats_data, last_update)
            except Exception as e:
                logger.error(f"Error in warexp command: {e}")
                return "\n[color=#FF0000]Error retrieving war statistics. Please try again.[/color]"
        
        # Exp deltas log command
        if msg.startswith("!explog"):
            try:
                parts = msg.split()
                
                # Default to 100 entries if no parameter
                if len(parts) < 2:
                    return "\n" + get_exp_log(minutes=None, entries=100)
                else:
                    minutes = int(parts[1])
                    if minutes < 1 or minutes > 1440:  # Max 24 hours
                        return "\n[color=#FF6B6B]Minutes must be between 1 and 1440.[/color]"
                    
                    return "\n" + get_exp_log(minutes=minutes)
            except ValueError:
                return "\n[color=#FF6B6B]Invalid number of minutes. Usage: !explog [minutes][/color]"
            except Exception as e:
                logger.error(f"Error in explog command: {e}")
                return "\n[color=#FF0000]Error retrieving exp log.[/color]"
        
        # Go home command - move user and bot to Djinns channel
        if msg.startswith("!bdsm"):
            try:
                if clid is None:
                    return "\n[color=#FF6B6B]Client ID not available.[/color]"
                
                # Get bot's own client ID
                try:
                    bdsm_text_async(nickname, bot, clid)
                    whoami = bot.worker_conn.whoami().parsed[0]
                    bot_clid = whoami.get('clid', '')
                except Exception as e:
                    logger.error(f"Error getting bot client ID: {e}")
                    return "\n[color=#FF0000]Error: Could not get bot client ID.[/color]"
                
                # Move both user and bot to Djinns
                success = bot.move_to_djinns(clid, bot_clid)
                
                if success:
                    # Start async API call that will send response when ready
                    
                    return "\n[b][color=#4ECDC4]Hmmmm...[/color][/b]"
                else:
                    return "\n[color=#FF6B6B]Failed to move to Djinns channel. Channel may not exist.[/color]"
                    
            except Exception as e:
                logger.error(f"Error in bdsm command: {e}")
                return "\n[color=#FF0000]Error executing bdsm command.[/color]"
        
        # Show logs command
        if msg.startswith("!resp "):
            import random
            resp_choices=[
                "Rotworm de Thais",
                "Larvas de Port Hope",
                "Dragons de Rookgard",
                "Undead Micropenis",
                "Xereca's Darklight",
                "Dp de thais andar de baixo",
                "Casa do caralho",
                "Cyclopolis de roshamuul"

            ]


            return f"[b][color=#228B22]O respawn ([b]{random.choice(resp_choices)}[/b]) é seu agora. O limite de tempo neste respawn é de [b]03:00[/b]"



        if msg.startswith("!showlogs"):
            try:
                if not hasattr(bot, 'log_handler'):
                    return "\n[color=#FF6B6B]Log handler is not available.[/color]"
                
                logs = bot.log_handler.get_logs(100)
                
                if not logs:
                    return "\n[color=#A0A0A0]No warnings or errors logged yet.[/color]"
                
                # Group consecutive identical errors
                grouped_logs = []
                for log in reversed(logs):
                    log_key = (log.get('level', ''), log.get('message', ''), log.get('module', ''))
                    
                    if grouped_logs and grouped_logs[-1]['key'] == log_key:
                        # Same as previous, increment count
                        grouped_logs[-1]['count'] += 1
                        grouped_logs[-1]['last_timestamp'] = log.get('timestamp', 'Unknown')
                    else:
                        # New entry
                        grouped_logs.append({
                            'key': log_key,
                            'timestamp': log.get('timestamp', 'Unknown'),
                            'last_timestamp': log.get('timestamp', 'Unknown'),
                            'level': log.get('level', 'UNKNOWN'),
                            'message': log.get('message', ''),
                            'module': log.get('module', ''),
                            'count': 1
                        })
                
                # Format output
                message = f"[b][color=#FFD700]═══ Bot Logs (Last {len(logs)} Entries) ═══[/color][/b]\n"
                message += "[color=#505050]" + "═" * 60 + "[/color]\n\n"
                
                # Display grouped logs
                for log in grouped_logs:
                    timestamp = log['timestamp']
                    last_timestamp = log['last_timestamp']
                    level = log['level']
                    log_message = log['message']
                    module = log['module']
                    count = log['count']
                    
                    # Color based on level
                    if level == 'ERROR' or level == 'CRITICAL':
                        level_color = '#FF6B6B'  # Red
                        msg_color = '#FFB3B3'    # Light red
                    elif level == 'WARNING':
                        level_color = '#FFD700'  # Gold
                        msg_color = '#FFEB99'    # Light gold
                    else:
                        level_color = '#A0A0A0'  # Gray
                        msg_color = '#D0D0D0'    # Light gray
                    
                    # Show time range if count > 1
                    if count > 1:
                        time_display = f"{last_timestamp} - {timestamp}"
                    else:
                        time_display = timestamp
                    
                    message += f"[color=#A0A0A0]{time_display}[/color] "
                    message += f"[b][color={level_color}]{level}[/color][/b] "
                    message += f"[color=#505050]({module})[/color]\n"
                    
                    # Add count if more than 1
                    if count > 1:
                        message += f"  [color={msg_color}]{log_message}[/color] [color=#00FF00](x{count})[/color]\n\n"
                    else:
                        message += f"  [color={msg_color}]{log_message}[/color]\n\n"
                
                message += "[color=#505050]" + "═" * 60 + "[/color]"
                return "\n" + message
            except Exception as e:
                logger.error(f"Error in showlogs command: {e}")
                return "\n[color=#FF0000]Error retrieving logs.[/color]"
    except Exception as e:
        logger.error(f"Error processing command: {e}")
        return "\n[color=#FF0000]QUEBREI: {e}.[/color]"
    
    # Unknown command




    t=get_txt()
    if isinstance(t, str) and t.strip():
        t=f"\n[color=#FF6B6B]{t.strip()}[/color]"
    else:
        t=""
    default_response = f"\n[color=#A0A0A0]Esse não é o x3tbot. Digite[/color] [b][color=#4ECDC4]!help[/color][/b] [color=#A0A0A0]para ver os comandos disponíveis[/color] {t}"
    
    return str(default_response)
