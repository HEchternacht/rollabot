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
            return "[color=#FF6B6B]Nenhum log de exp de guerra encontrado.[/color]"
        
        # Read all rows
        rows = []
        with open(exps_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)
        
        if not rows:
            return "[color=#FF6B6B]Nenhum dado de exp de guerra disponível.[/color]"
        
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
        message = f"[b][color=#FFD700]═══ Log de Exp de Guerra (Últimos {days} Dias) ═══[/color][/b]\n"
        message += "[color=#505050]" + "═" * 60 + "[/color]\n\n"
        
        for row in rows:
            date = row.get('date', 'Desconhecido')
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
        return "[color=#FF0000]Erro ao ler log de exp de guerra.[/color]"


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
                return f"[color=#FF6B6B]Nenhum delta de exp nos últimos {minutes} minutos.[/color]"
            
            header = f"[b][color=#FFD700]═══ Deltas de Exp (Últimos {minutes} Minutos) ═══[/color][/b]"
        else:
            # Get last N entries
            rows = all_rows[-entries:] if entries < len(all_rows) else all_rows
            header = f"[b][color=#FFD700]═══ Deltas de Exp (Últimas {len(rows)} Entradas) ═══[/color][/b]"
        
        # Format output - simple list format
        message = header + "\n"
        message += "[color=#505050]" + "═" * 60 + "[/color]\n\n"
        
        # Show in reverse order (newest first)
        for row in reversed(rows):
            timedate = row.get('timedate', 'Desconhecido')
            name = row.get('name', 'Desconhecido')
            exp = row.get('exp', '0')
            
            # Format: <daytime> <name> <deltexp>
            message += f"[color=#A0A0A0]{timedate}[/color] [color=#4ECDC4]{name}[/color] [color=#00FF00]{exp}[/color]\n"
        
        message += "\n[color=#505050]" + "═" * 60 + "[/color]"
        return message
    except Exception as e:
        logger.error(f"Error reading exp deltas log: {e}", exc_info=True)
        return "[color=#FF0000]Erro ao ler log de deltas de exp.[/color]"


def register_exp_user(uid: str, min_exp: int = 0) -> str:
    """
    Register a user for guild exp notifications with optional minimum exp threshold.
    
    Args:
        uid: User UID to register
        min_exp: Minimum exp delta to receive notifications (default: 0, all notifications)
    
    Returns:
        str: Success or error message
    """
    try:
        log_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        registered_file = os.path.join(log_dir, 'registered.txt')
        logger.debug(f"Registering UID: {uid} with min_exp: {min_exp} in file: {registered_file}")
        
        # Load existing registrations
        registered_users = {}  # uid -> min_exp
        if os.path.exists(registered_file):
            with open(registered_file, 'r', encoding='utf-8') as f:
                logger.debug(f"Reading existing registered users from {registered_file}")
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    
                    # Parse format: "uid" or "uid,min_exp"
                    if ',' in line:
                        parts = line.split(',', 1)
                        registered_users[parts[0]] = int(parts[1])
                    else:
                        # Backward compatibility: no threshold means 0
                        registered_users[line] = 0
        
        # Add or update user registration
        registered_users[uid] = min_exp
        
        # Write back to file
        with open(registered_file, 'w', encoding='utf-8') as f:
            for reg_uid in sorted(registered_users.keys()):
                threshold = registered_users[reg_uid]
                f.write(f"{reg_uid},{threshold}\n")
        
        if min_exp > 0:
            min_exp_formatted = f"{min_exp:,}"
            logger.info(f"Registered {uid} for guild exp notifications (min: {min_exp})")
            return f"[b][color=#00FF00]✅ Registrado com sucesso para notificações de exp da guilda![/color][/b]\n[color=#FFD700]Limiar de exp mínima: {min_exp_formatted}[/color]"
        else:
            logger.info(f"Registered {uid} for guild exp notifications (all notifications)")
            return "[b][color=#00FF00]✅ Registrado com sucesso para notificações de exp da guilda![/color][/b]\n[color=#FFD700]Você receberá todas as notificações de exp.[/color]"
        
    except Exception as e:
        logger.error(f"Error registering user for exp notifications: {e}")
        return f"[color=#FF0000]Erro ao registrar: {str(e)}[/color]"


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
            return "[color=#FFD700]Você não está registrado para notificações de exp da guilda.[/color]"
        
        # Load existing registrations
        registered_users = {}  # uid -> min_exp
        with open(registered_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                # Parse format: "uid" or "uid,min_exp"
                if ',' in line:
                    parts = line.split(',', 1)
                    registered_users[parts[0]] = int(parts[1])
                else:
                    # Backward compatibility: no threshold means 0
                    registered_users[line] = 0
        
        # Check if registered
        if uid not in registered_users:
            return "[color=#FFD700]Você não está registrado para notificações de exp da guilda.[/color]"
        
        # Remove user
        del registered_users[uid]
        
        # Write back to file
        with open(registered_file, 'w', encoding='utf-8') as f:
            for reg_uid in sorted(registered_users.keys()):
                threshold = registered_users[reg_uid]
                f.write(f"{reg_uid},{threshold}\n")
        
        logger.info(f"Unregistered {uid} from guild exp notifications")
        return "[b][color=#00FF00]✅ Desregistrado com sucesso das notificações de exp da guilda.[/color][/b]"
        
    except Exception as e:
        logger.error(f"Error unregistering user from exp notifications: {e}")
        return f"[color=#FF0000]Erro ao desregistrar: {str(e)}[/color]"


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
        return "[color=#FF6B6B]Nenhuma estatística de guerra disponível ainda. Aguarde a primeira coleta de dados.[/color]"
    
    try:
        shell_members = data.get("shell", {}).get("members", [])
        ascended_members = data.get("ascended", {}).get("members", [])
        shell_deaths = len([m for m in shell_members if m.get("delta", 0) < 0])
        ascended_deaths = len([m for m in ascended_members if m.get("delta", 0) < 0])

        result = "[b][color=#FFD700]═══ ESTATÍSTICAS DE GUERRA ═══[/color][/b]\n"
        result += (
            f"[b][color=#FF6B9D]Ascendant[/color] "
            f"[color=#FFD700]{shell_deaths}[/color] x "
            f"[color=#FFD700]{ascended_deaths}[/color] "
            f"[color=#4ECDC4]ShellPatrocina[/color][/b]\n"
            "\n"
            "[i]*Este score não é de kills, mas sim de membros que perderam exp (delta negativo) do time oposto*[/i]\n"
        )
        if last_update:
            result += f"[color=#A0A0A0]Última Atualização: {last_update.strftime('%Y-%m-%d %H:%M:%S')}[/color]\n"
        result += "[color=#505050]" + "═" * 50 + "[/color]\n\n"
        
        for key in ["shell", "ascended"]:
            g = data.get(key, {})
            guild_color = "#4ECDC4" if key == "shell" else "#FF6B9D"
            
            result += f"[b][color={guild_color}]⚔️ Guild: {key.upper()}[/color][/b]\n"
            result += f"[color=#90EE90]● Online:[/color] [b]{g.get('totalOnline', 0)}[/b]\n"
            result += f"[color=#98D8C8]▲ Total Ganho:[/color] [b]{g.get('totalGained', 0):,}[/b]\n"
            result += f"[color=#FF7F7F]▼ Total Perdido:[/color] [b]{g.get('totalLost', 0):,}[/b]\n"
            
            net = g.get('totalGained', 0) - g.get('totalLost', 0)
            net_color = "#00FF00" if net > 0 else "#FF0000" if net < 0 else "#FFFFFF"
            result += f"[color=#FFD700]═ Líquido:[/color] [b][color={net_color}]{net:+,}[/color][/b]\n\n"
            
            membros = g.get("members", [])
            
            # Top 3 gains
            gainers = sorted([m for m in membros if m.get("delta", 0) > 0], 
                           key=lambda x: -x.get("delta", 0))[:3]
            if gainers:
                result += "[b][color=#00FF00]🏆 Top 3 Ganhos:[/color][/b]\n"
                for i, m in enumerate(gainers, 1):
                    result += f"  [color=#FFD700]{i}.[/color] [b]{m.get('name', 'Desconhecido')}[/b] - LV {m.get('level', '?')} - [color=#00FF00]Δ {m.get('delta', 0):+,}[/color]\n"
                result += "\n"
            
            # Top 3 losses
            losers = sorted([m for m in membros if m.get("delta", 0) < 0], 
                          key=lambda x: x.get("delta", 0))[:3]
            if losers:
                result += "[b][color=#FF6B6B]💀 Top 3 Perdas:[/color][/b]\n"
                for i, m in enumerate(losers, 1):
                    result += f"  [color=#FFD700]{i}.[/color] [b]{m.get('name', 'Desconhecido')}[/b] - LV {m.get('level', '?')} - [color=#FF6B6B]Δ {m.get('delta', 0):+,}[/color]\n"
                result += "\n"
            
            result += "[color=#505050]" + "─" * 50 + "[/color]\n\n"
        
        return result
        
    except Exception as e:
        logger.error(f"Error formatting war stats: {e}")
        return f"[color=#FF0000]Erro ao formatar estatísticas de guerra: {str(e)}[/color]"


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
            return f"[color=#FF6B6B]Nenhuma atividade encontrada nos últimos {minutes} minuto(s).[/color]"
        
        # Format results
        result = f"[b][color=#4ECDC4]📋 Atividade dos últimos {minutes} minuto(s)[/color][/b] [color=#A0A0A0]({len(matches)} eventos)[/color]\n"
        result += "[color=#505050]" + "═" * 50 + "[/color]\n\n"
        
        for i, match in enumerate(matches, 1):
            timestamp = match.get('TIMESTAMP', '')
            uid = match.get('UID', '').strip()
            event = match.get('EVENT', 'unknown event')
            
            # Get nickname from mapping, fallback to full UID
            nickname = uid_to_nickname.get(uid, uid)
            
            result += f"[color=#FFD700]{i}.[/color] [color=#90EE90][{timestamp}][/color] [b]{nickname}[/b]\n"
            result += f"   [color=#FFFFFF]{event}[/color]\n\n"
        
        if len(matches) == max_results:
            result += f"[color=#A0A0A0](Mostrando primeiros {max_results} resultados)[/color]"
        
        return result
        
    except Exception as e:
        logger.error(f"Error retrieving recent logs: {e}")
        return f"[color=#FF0000]Erro ao recuperar logs: {str(e)}[/color]"


def get_registered_count():
    """
    Get the list of users registered for exp notifications with their nicknames.
    
    Returns:
        str: Formatted list of registered users or error
    """
    try:
        log_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        registered_file = os.path.join(log_dir, 'registered.txt')
        clients_ref_path = os.path.join(log_dir, 'clients_reference.csv')
        uid_nicknames_path = os.path.join(log_dir, 'uid_nicknames.csv')
        
        if not os.path.exists(registered_file):
            return "[color=#FF6B6B]📋 Nenhum usuário registrado para notificações de exp.[/color]"
        
        # Load registered UIDs
        registered_uids = []
        with open(registered_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                # Parse format: "uid" or "uid,min_exp"
                if ',' in line:
                    uid = line.split(',', 1)[0]
                else:
                    uid = line
                registered_uids.append(uid)
        
        if not registered_uids:
            return "[color=#FF6B6B]📋 Nenhum usuário registrado para notificações de exp.[/color]"
        
        # Build UID to nickname mapping
        uid_to_nickname = {}
        
        # Try uid_nicknames.csv first (historical data with all nicknames)
        if os.path.exists(uid_nicknames_path):
            with open(uid_nicknames_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    uid = row.get('UID', '').strip()
                    nickname = row.get('NICKNAME', '').strip()
                    if uid and nickname and uid not in uid_to_nickname:
                        # Use first nickname found
                        uid_to_nickname[uid] = nickname
        
        # Try clients_reference.csv (current data) to update/add missing
        if os.path.exists(clients_ref_path):
            with open(clients_ref_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    uid = row.get('uid', '').strip()
                    nickname = row.get('nickname', '').strip()
                    if uid and nickname:
                        uid_to_nickname[uid] = nickname  # Override with current nickname
        
        # Format output
        result = f"[b][color=#4ECDC4]📋 Usuários registrados para notificações de exp: {len(registered_uids)}[/color][/b]\n"
        
        # Get nicknames for registered users
        nicknames = []
        for uid in registered_uids:
            nickname = uid_to_nickname.get(uid, uid)  # Fallback to full UID
            nicknames.append(nickname)
        
        # Sort alphabetically
        nicknames.sort()
        
        # Display as comma-separated list
        result += "[color=#FFD700]" + ",\n".join(nicknames) + "[/color]"
        
        return result
    except Exception as e:
        logger.error(f"Error getting registered count: {e}")
        return f"[color=#FF0000]Erro: {str(e)}[/color]"


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
        return f"[b][color=#4ECDC4]⏱️ Tempo online do bot:[/color][/b] [color=#FFD700]{days}d {hours}h {minutes}m {seconds}s[/color]"
    return "[color=#FF6B6B]⏱️ Informações de tempo online não disponíveis.[/color]"


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
            return "[color=#FF6B6B]Nenhum dado de usuário disponível.[/color]"
        
        # Filter for plus mode (only users with >1 nickname)
        if plus_mode:
            uid_nicknames = {uid: nicks for uid, nicks in uid_nicknames.items() if len(nicks) > 1}
            
            if not uid_nicknames:
                return "[color=#FF6B6B]Nenhum usuário com múltiplos nicknames encontrado.[/color]"
        
        # Format results
        mode_text = " Plus" if plus_mode else ""
        result = f"[b][color=#4ECDC4]👥 Lista de Usuários{mode_text}[/color][/b] [color=#A0A0A0]({len(uid_nicknames)} UIDs únicos)[/color]\n"
        result += "[color=#505050]" + "═" * 50 + "[/color]\n\n"
        
        for uid, nicknames in sorted(uid_nicknames.items()):
            # Show full UID
            uid_display = uid
            nicks_list = ', '.join(sorted(nicknames))
            
            # Add nickname count if more than 1
            if len(nicknames) > 1:
                result += f"[color=#90EE90]{uid_display}[/color] [color=#FFD700]→[/color] [b]{nicks_list}[/b] [color=#FF69B4]({len(nicknames)} nomes)[/color]\n"
            else:
                result += f"[color=#90EE90]{uid_display}[/color] [color=#FFD700]→[/color] [b]{nicks_list}[/b]\n"
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting users list: {e}")
        return f"[color=#FF0000]Erro: {str(e)}[/color]"


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
            return "[color=#FF6B6B]Nenhum dado de canal disponível.[/color]"
        
        # Format results
        result = f"[b][color=#4ECDC4]📋 Lista de Canais[/color][/b] [color=#A0A0A0]({len(channels)} canais)[/color]\n"
        result += "[color=#505050]" + "═" * 50 + "[/color]\n\n"
        
        for cid, channel_name in sorted(channels.items(), key=lambda x: int(x[0]) if x[0].isdigit() else 0):
            result += f"[color=#90EE90]{cid}[/color] [color=#FFD700]→[/color] [b]{channel_name}[/b]\n"
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting channel list: {e}")
        return f"[color=#FF0000]Erro: {str(e)}[/color]"


def get_pkc_logs(search_term=None, max_results=50):
    """
    Get PKC event kick logs, optionally filtered by nickname or clid.
    
    Args:
        search_term: Optional nickname or clid to filter by
        max_results: Maximum number of results to return (default: 50)
    
    Returns:
        str: Formatted PKC logs
    """
    try:
        log_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        pkc_log_path = os.path.join(log_dir, 'pkc.csv')
        
        if not os.path.exists(pkc_log_path):
            return "[color=#FF6B6B]Nenhum log PKC encontrado.[/color]"
        
        # Read all rows
        rows = []
        with open(pkc_log_path, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Filter by search term if provided
                if search_term:
                    search_lower = search_term.lower()
                    nickname_lower = row.get('nickname', '').lower()
                    clid = row.get('clid', '')
                    
                    if search_lower not in nickname_lower and search_lower != clid:
                        continue
                
                rows.append(row)
        
        if not rows:
            if search_term:
                return f"[color=#FF6B6B]Nenhum log PKC encontrado para '{search_term}'.[/color]"
            else:
                return "[color=#FF6B6B]Nenhum log PKC disponível.[/color]"
        
        # Reverse to show most recent first and limit results
        rows.reverse()
        rows = rows[:max_results]
        
        # Format results
        if search_term:
            result = f"[b][color=#DC143C]🔒 Logs PKC para '{search_term}'[/color][/b] [color=#A0A0A0]({len(rows)} entradas)[/color]\n"
        else:
            result = f"[b][color=#DC143C]🔒 Logs PKC Recentes[/color][/b] [color=#A0A0A0]({len(rows)} entradas)[/color]\n"
        
        result += "[color=#505050]" + "═" * 60 + "[/color]\n\n"
        
        for row in rows:
            datetime_str = row.get('datetime', 'N/A')
            channel_id = row.get('channel_id', 'N/A')
            clid = row.get('clid', 'N/A')
            nickname = row.get('nickname', 'Desconhecido')
            
            result += f"[color=#A0A0A0]{datetime_str}[/color]\n"
            result += f"  [color=#4ECDC4]{nickname}[/color] [color=#505050](clid: {clid})[/color]\n"
            result += f"  [color=#FFD700]Canal:[/color] {channel_id}\n\n"
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting PKC logs: {e}")
        return f"[color=#FF0000]Erro: {str(e)}[/color]"


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
        
        # Warn all users currently in the channel (they have 30s to leave)
        # Queue a request to get users in channel and warn them
        bot.command_queue.put({
            'type': 'pkc_warn_initial_users',
            'channel_id': channel_id
        })
        logger.info(f"PKC {thread_id}: Queued initial user warnings for channel {channel_id}")
        
        # Monitor loop
        while time.time() < end_time:
            time.sleep(check_interval)
            
            # Check if still running (in case bot is shutting down)
            if not bot._running:
                break
            
            # Check if cancelled via !cancelpkc by checking if channel still exists
            if channel_id not in bot.active_pkc_channels:
                logger.info(f"PKC {thread_id}: Channel {channel_id} monitoring cancelled")
                return  # Exit thread early
            
            # Note: PKC now works entirely through event-driven warnings
            # Users entering the channel trigger warnings via event handler
            # No periodic polling needed - all operations go through worker queue
        
        remaining_time = max(0, int((end_time - time.time()) / 60))
        if remaining_time == 0:
            logger.info(f"PKC {thread_id}: Channel {channel_id} monitoring completed after {duration_minutes} minutes")
        else:
            logger.info(f"PKC {thread_id}: Channel {channel_id} monitoring stopped early")
        
    except Exception as e:
        logger.error(f"PKC {thread_id}: Error in channel monitoring thread: {e}", exc_info=True)
    
    finally:
        # Queue removal from active monitoring
        bot.command_queue.put({
            'type': 'pkc_cleanup_channel',
            'channel_id': channel_id,
            'thread_id': thread_id
        })


def calculate_user_statistics(uid: str) -> dict:
    """
    Calculate comprehensive statistics for a user based on activity log.
    
    Args:
        uid: User UID to calculate statistics for
    
    Returns:
        dict: Statistics including time monitored, mute times, and channel times
    """
    try:
        log_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        readable_log_path = os.path.join(log_dir, 'activity_log_readable.csv')
        
        if not os.path.exists(readable_log_path):
            return {}
        
        # Read all events for this user
        user_events = []
        with open(readable_log_path, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('UID', '').strip() == uid:
                    user_events.append(row)
        
        if not user_events:
            return {}
        
        # Initialize tracking variables
        current_channel = None
        current_input_muted = False
        current_output_muted = False
        is_connected = False
        
        channel_times = {}  # channel -> total seconds
        input_muted_time = 0
        input_unmuted_time = 0
        output_muted_time = 0
        output_unmuted_time = 0
        
        last_timestamp = None
        first_timestamp = None
        
        for event in user_events:
            timestamp_str = event.get('TIMESTAMP', '')
            event_desc = event.get('EVENT', '')
            
            try:
                timestamp = datetime.strptime(timestamp_str, '%d/%m/%Y-%H:%M:%S')
            except ValueError:
                continue
            
            # Calculate time delta if we have a previous timestamp
            if last_timestamp and is_connected:
                time_delta = (timestamp - last_timestamp).total_seconds()
                
                # Add time to current channel
                if current_channel:
                    if current_channel not in channel_times:
                        channel_times[current_channel] = 0
                    channel_times[current_channel] += time_delta
                
                # Add time to mute status
                if current_input_muted:
                    input_muted_time += time_delta
                else:
                    input_unmuted_time += time_delta
                
                if current_output_muted:
                    output_muted_time += time_delta
                else:
                    output_unmuted_time += time_delta
            
            # Update state based on event
            if 'Connected to server' in event_desc:
                is_connected = True
                if first_timestamp is None:
                    first_timestamp = timestamp
            elif 'Disconnected' in event_desc:
                is_connected = False
                current_channel = None
            elif 'Moved from' in event_desc and 'to' in event_desc:
                # Extract destination channel
                parts = event_desc.split(' to ')
                if len(parts) == 2:
                    current_channel = parts[1].strip()
            elif 'Muted input microphone' in event_desc:
                current_input_muted = True
            elif 'Unmuted input microphone' in event_desc:
                current_input_muted = False
            elif 'Muted output speakers' in event_desc:
                current_output_muted = True
            elif 'Unmuted output speakers' in event_desc:
                current_output_muted = False
            
            last_timestamp = timestamp
        
        # Calculate total time monitored
        total_time = 0
        if first_timestamp and last_timestamp:
            total_time = (last_timestamp - first_timestamp).total_seconds()
        
        return {
            'total_time': total_time,
            'input_muted_time': input_muted_time,
            'input_unmuted_time': input_unmuted_time,
            'output_muted_time': output_muted_time,
            'output_unmuted_time': output_unmuted_time,
            'channel_times': channel_times
        }
    
    except Exception as e:
        logger.error(f"Error calculating user statistics: {e}")
        return {}


def format_time(seconds: float) -> str:
    """Format seconds into human-readable time string."""
    if seconds < 60:
        return f"{int(seconds)}s"
    
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    
    if hours > 0:
        return f"{hours}h:{minutes:02d}m"
    else:
        return f"{minutes}m"


def search_activity_log(search_term: str, max_results: int = 50):
    """
    Search human-readable activity log for entries matching uid, nickname, or ip.
    
    Args:
        search_term: The uid, nickname, or ip to search for
        max_results: Maximum number of results to return (default: 50, shows LAST entries)
                    Use None or -1 to show ALL results
    
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
        # Priority: exact match > case-insensitive match > partial match
        target_uids = set()
        matched_user_info = {}  # uid -> (nickname, ip)
        
        # Collect all rows from both sources first
        all_rows = []
        
        # Check clients_reference.csv first (more up-to-date)
        if os.path.exists(clients_ref_path):
            with open(clients_ref_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    uid = row.get('uid', '')
                    nickname = row.get('nickname', '')
                    ip = row.get('ip', '')
                    all_rows.append(('clients_ref', uid, nickname, ip))
        
        # Also check users_seen.csv for historical data
        if os.path.exists(users_seen_path):
            with open(users_seen_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    uid = row.get('UID', '')
                    nickname = row.get('NICKNAME', '')
                    ip = row.get('IP', '')
                    all_rows.append(('users_seen', uid, nickname, ip))
        
        # Pass 1: Exact case-sensitive match
        for source, uid, nickname, ip in all_rows:
            if search_term == uid or search_term == nickname or search_term == ip:
                target_uids.add(uid)
                matched_user_info[uid] = (nickname, ip)
        
        # Pass 2: Case-insensitive exact match (only if no exact match found)
        if not target_uids:
            for source, uid, nickname, ip in all_rows:
                if (search_term.lower() == uid.lower() or 
                    search_term.lower() == nickname.lower() or 
                    search_term.lower() == ip.lower()):
                    target_uids.add(uid)
                    matched_user_info[uid] = (nickname, ip)
        
        # Pass 3: Partial substring match (only if no exact match found)
        if not target_uids:
            for source, uid, nickname, ip in all_rows:
                if (search_term.lower() in uid.lower() or
                    search_term.lower() in nickname.lower() or
                    search_term in ip):
                    target_uids.add(uid)
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
            return f"\n[color=#FF6B6B]Nenhuma atividade encontrada para: {search_term}[/color]"
        
        # Take only the LAST max_results entries (or all if max_results is None or -1)
        if max_results is None or max_results == -1:
            matches = all_matches
        else:
            matches = all_matches[-max_results:]
        total_found = len(all_matches)
        
        # Format results
        user_display = search_term
        if len(target_uids) == 1:
            uid = list(target_uids)[0]
            if uid in matched_user_info:
                nickname, ip = matched_user_info[uid]
                user_display = f"{nickname} ({uid})"
        
        showing_text = "mostrando todas" if (max_results is None or max_results == -1) else f"mostrando últimas {len(matches)}"
        result = f"[b][color=#4ECDC4]🔍 Encontradas {total_found} atividades para '[/color][color=#FFD700]{user_display}[/color][color=#4ECDC4]'[/color][/b] [color=#A0A0A0]({showing_text})[/color]\n\n"
        for i, match in enumerate(matches, 1):
            timestamp = match.get('TIMESTAMP', '')
            event = match.get('EVENT', 'unknown event')
            
            result += f"[color=#FFD700]{i}.[/color] [color=#90EE90][{timestamp}][/color] {event}\n"
        
        if max_results and max_results != -1 and total_found > max_results:
            result += f"\n[color=#A0A0A0](Showing last {max_results} of {total_found} total results)[/color]"
        
        # Add comprehensive statistics at the bottom
        if len(target_uids) == 1:
            uid = list(target_uids)[0]
            stats = calculate_user_statistics(uid)
            
            if stats and stats.get('total_time', 0) > 0:
                result += "\n\n[b][color=#4ECDC4]═══════════════════════════════[/color][/b]\n"
                result += "[b][color=#FFD700]📊 USER STATISTICS[/color][/b]\n"
                result += "[b][color=#4ECDC4]═══════════════════════════════[/color][/b]\n\n"
                
                # Total time monitored
                total_time = stats['total_time']
                result += f"[b][color=#90EE90]⏱️ Total Time Monitored:[/color][/b] [color=#FFFFFF]{format_time(total_time)}[/color]\n\n"
                
                # Input mute statistics
                input_muted = stats['input_muted_time']
                input_unmuted = stats['input_unmuted_time']
                input_total = input_muted + input_unmuted
                
                if input_total > 0:
                    result += "[b][color=#FF69B4]🎤 INPUT (Microphone):[/color][/b]\n"
                    muted_pct = (input_muted / input_total) * 100
                    unmuted_pct = (input_unmuted / input_total) * 100
                    result += f"  [color=#FF6B6B]Muted:[/color] {format_time(input_muted)} [color=#A0A0A0]({muted_pct:.1f}%)[/color]\n"
                    result += f"  [color=#00FF00]Unmuted:[/color] {format_time(input_unmuted)} [color=#A0A0A0]({unmuted_pct:.1f}%)[/color]\n\n"
                
                # Output mute statistics
                output_muted = stats['output_muted_time']
                output_unmuted = stats['output_unmuted_time']
                output_total = output_muted + output_unmuted
                
                if output_total > 0:
                    result += "[b][color=#4ECDC4]🔊 OUTPUT (Speakers):[/color][/b]\n"
                    muted_pct = (output_muted / output_total) * 100
                    unmuted_pct = (output_unmuted / output_total) * 100
                    result += f"  [color=#FF6B6B]Muted:[/color] {format_time(output_muted)} [color=#A0A0A0]({muted_pct:.1f}%)[/color]\n"
                    result += f"  [color=#00FF00]Unmuted:[/color] {format_time(output_unmuted)} [color=#A0A0A0]({unmuted_pct:.1f}%)[/color]\n\n"
                
                # Channel time statistics (sorted by time spent)
                channel_times = stats.get('channel_times', {})
                if channel_times:
                    result += "[b][color=#FFD700]📍 TIME BY CHANNEL (Ordered by time spent):[/color][/b]\n"
                    
                    # Sort channels by time spent (descending)
                    sorted_channels = sorted(channel_times.items(), key=lambda x: x[1], reverse=True)
                    
                    total_channel_time = sum(channel_times.values())
                    
                    for channel_name, time_spent in sorted_channels:
                        percentage = (time_spent / total_channel_time) * 100 if total_channel_time > 0 else 0
                        result += f"  [color=#4ECDC4]{channel_name}:[/color] {format_time(time_spent)} [color=#A0A0A0]({percentage:.1f}%)[/color]\n"
                
                result += "\n[b][color=#4ECDC4]═══════════════════════════════[/color][/b]"
        
        return result
        
    except Exception as e:
        logger.error(f"Error searching activity log: {e}")
        return f"\n[color=#FF0000]Error searching log: {str(e)}[/color]"


def search_exp_log(search_term: str, max_results: int = 50):
    """
    Search exp deltas log for entries matching a player name.
    
    Args:
        search_term: The name to search for
        max_results: Maximum number of results to return (default: 50, shows LAST entries)
                    Use None or -1 to show ALL results
    
    Returns:
        str: Formatted results or error message
    """
    try:
        # Get log file path (in project root)
        log_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        exp_deltas_file = os.path.join(log_dir, 'exp_deltas.csv')
        
        if not os.path.exists(exp_deltas_file):
            return "[color=#FF6B6B]No exp deltas log found.[/color]"
        
        # Read all rows and search for matching names
        # Priority: exact match > case-insensitive match > partial match
        all_rows = []
        with open(exp_deltas_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                all_rows.append(row)
        
        if not all_rows:
            return "[color=#FF6B6B]No exp deltas available.[/color]"
        
        # Pass 1: Exact case-sensitive match
        exact_matches = []
        for row in all_rows:
            name = row.get('name', '')
            if search_term == name:
                exact_matches.append(row)
        
        # Pass 2: Case-insensitive exact match (only if no exact match found)
        if not exact_matches:
            for row in all_rows:
                name = row.get('name', '')
                if search_term.lower() == name.lower():
                    exact_matches.append(row)
        
        # Pass 3: Partial substring match (only if no exact match found)
        if not exact_matches:
            for row in all_rows:
                name = row.get('name', '')
                if search_term.lower() in name.lower():
                    exact_matches.append(row)
        
        if not exact_matches:
            return f"\n[color=#FF6B6B]Nenhum delta de exp encontrado para: {search_term}[/color]"
        
        # Take only the LAST max_results entries (or all if max_results is None or -1)
        if max_results is None or max_results == -1:
            matches = exact_matches
        else:
            matches = exact_matches[-max_results:]
        total_found = len(exact_matches)
        
        # Calculate total exp
        total_exp = sum(int(row.get('exp', 0)) for row in exact_matches)
        
        # Format results
        showing_text = "mostrando todos" if (max_results is None or max_results == -1) else f"mostrando últimos {len(matches)}"
        result = f"[b][color=#4ECDC4]🔍 Encontrados {total_found} ganhos de exp para '[/color][color=#FFD700]{search_term}[/color][color=#4ECDC4]'[/color][/b] [color=#A0A0A0]({showing_text})[/color]\n"
        result += f"[b][color=#90EE90]Total Exp:[/color][/b] [color=#00FF00]{total_exp:,}[/color]\n\n"
        
        # Show entries in reverse order (newest first)
        for i, match in enumerate(reversed(matches), 1):
            timedate = match.get('timedate', 'Unknown')
            name = match.get('name', 'Unknown')
            exp = match.get('exp', '0')
            
            result += f"[color=#FFD700]{i}.[/color] [color=#A0A0A0]{timedate}[/color] [color=#4ECDC4]{name}[/color] [color=#00FF00]{exp}[/color]\n"
        
        if max_results and max_results != -1 and total_found > max_results:
            result += f"\n[color=#A0A0A0](Showing last {max_results} of {total_found} total results)[/color]"
        
        return result
        
    except Exception as e:
        logger.error(f"Error searching exp log: {e}")
        return f"\n[color=#FF0000]Error searching exp log: {str(e)}[/color]"


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
                            {"role": "system", "content": "Você é um escritor de contos eróticos e romanticos, pesquise na internet por contos ja criados e use como inspiração, tente ser o maximo explicito, você deve escrever somente o conto, não use frases como 'posso te ajudar em algo mais', no final do texto"},
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
    # Define help texts as constants for reuse
    HELP_WAREXP = (
        "\n"
        "[b][color=#9932CC]═══════════════════════════════════════════════[/color][/b]\n"
        "[b][color=#FF69B4]📊 Ajuda Detalhada - Comandos de Guerra[/color][/b]\n"
        "[b][color=#9932CC]═══════════════════════════════════════════════[/color][/b]\n\n"
        "[b][color=#9932CC]!warexp[/color][/b]\n"
        "[color=#A0A0A0]Mostra as estatísticas atuais da guerra entre Shell e Ascendant.[/color]\n"
        "[color=#90EE90]Exemplo:[/color] !warexp\n\n"
        "[b][color=#FF1493]!warexplog[/color][/b] [color=#A0A0A0][dias][/color]\n"
        "[color=#A0A0A0]Mostra o histórico de exp de guerra dos últimos N dias (padrão: 30).[/color]\n"
        "[color=#A0A0A0]Exibe data, exp e score de cada guilda por dia.[/color]\n"
        "[color=#90EE90]Exemplos:[/color]\n"
        "  !warexplog       (últimos 30 dias)\n"
        "  !warexplog 7     (últimos 7 dias)\n"
        "  !warexplog 90    (últimos 90 dias, máx: 365)\n\n"
        "[color=#FFD700]💡 Dica:[/color] [color=#A0A0A0]Use !warexplog para ver tendências e !warexp para dados em tempo real.[/color]\n"
        "[b][color=#9932CC]═══════════════════════════════════════════════[/color][/b]"
    )
    
    HELP_LOGGER = (
        "\n"
        "[b][color=#FFD700]═══════════════════════════════════════════════[/color][/b]\n"
        "[b][color=#FF69B4]📝 Ajuda Detalhada - Comandos de Log[/color][/b]\n"
        "[b][color=#FFD700]═══════════════════════════════════════════════[/color][/b]\n\n"
        "[b][color=#FFD700]!logger[/color][/b] [color=#A0A0A0]<uid/nickname/ip> [all][/color]\n"
        "[color=#A0A0A0]Busca no registro de atividades por UID, nickname ou IP.[/color]\n"
        "[color=#A0A0A0]Por padrão mostra últimas 50 entradas. Use 'all' para ver todas.[/color]\n"
        "[color=#90EE90]Exemplos:[/color]\n"
        "  !logger john           (últimas 50 entradas)\n"
        "  !logger john all       (todas as entradas)\n"
        "  !logger 192.168.1.1    (busca por IP)\n\n"
        "[b][color=#9ACD32]!lastminuteslogs[/color][/b] [color=#A0A0A0]<minutos>[/color]\n"
        "[color=#A0A0A0]Mostra atividades dos últimos N minutos (máx: 1440 = 24h).[/color]\n"
        "[color=#90EE90]Exemplos:[/color]\n"
        "  !lastminuteslogs 5     (últimos 5 minutos)\n"
        "  !lastminuteslogs 60    (última hora)\n\n"
        "[b][color=#FF8C00]!showlogs[/color][/b]\n"
        "[color=#A0A0A0]Mostra os últimos 100 avisos/erros do bot.[/color]\n"
        "[color=#A0A0A0]Útil para diagnosticar problemas.[/color]\n"
        "[color=#90EE90]Exemplo:[/color] !showlogs\n\n"
        "[color=#FFD700]💡 Dica:[/color] [color=#A0A0A0]Use !logger para rastrear jogadores específicos e !lastminuteslogs para ver atividade recente geral.[/color]\n"
        "[b][color=#FFD700]═══════════════════════════════════════════════[/color][/b]"
    )
    
    HELP_EXP = (
        "\n"
        "[b][color=#4169E1]═══════════════════════════════════════════════[/color][/b]\n"
        "[b][color=#FF69B4]🔔 Ajuda Detalhada - Sistema de Notificações de EXP[/color][/b]\n"
        "[b][color=#4169E1]═══════════════════════════════════════════════[/color][/b]\n\n"
        "[b][color=#4169E1]!registerexp[/color][/b] [color=#A0A0A0][min_exp][/color]\n"
        "[color=#A0A0A0]Registra você para receber notificações quando a guilda ganhar exp.[/color]\n"
        "[color=#A0A0A0]Opcionalmente defina um exp mínimo para ser notificado.[/color]\n"
        "[color=#90EE90]Exemplos:[/color]\n"
        "  !registerexp           (notifica qualquer ganho)\n"
        "  !registerexp 100000    (notifica ganhos ≥ 100k)\n"
        "  !registerexp 500000    (notifica ganhos ≥ 500k)\n\n"
        "[b][color=#8A2BE2]!unregisterexp[/color][/b]\n"
        "[color=#A0A0A0]Remove você das notificações de exp da guilda.[/color]\n"
        "[color=#90EE90]Exemplo:[/color] !unregisterexp\n\n"
        "[b][color=#00CED1]!registered[/color][/b]\n"
        "[color=#A0A0A0]Mostra quantos usuários estão registrados para notificações.[/color]\n"
        "[color=#90EE90]Exemplo:[/color] !registered\n\n"
        "[b][color=#FF4500]!explog[/color][/b] [color=#A0A0A0][minutos][/color]\n"
        "[color=#A0A0A0]Mostra ganhos recentes de exp (padrão: últimas 100 entradas).[/color]\n"
        "[color=#A0A0A0]Use parâmetro de minutos para filtrar por tempo (máx: 1440).[/color]\n"
        "[color=#90EE90]Exemplos:[/color]\n"
        "  !explog         (últimas 100 entradas)\n"
        "  !explog 30      (últimos 30 minutos)\n\n"
        "[b][color=#FF6347]!explogger[/color][/b] [color=#A0A0A0]<nome> [all][/color]\n"
        "[color=#A0A0A0]Busca ganhos de exp por nome de jogador.[/color]\n"
        "[color=#A0A0A0]Por padrão mostra últimas 50 entradas. Use 'all' para ver todas.[/color]\n"
        "[color=#90EE90]Exemplos:[/color]\n"
        "  !explogger john      (últimas 50)\n"
        "  !explogger john all  (todas)\n\n"
        "[color=#FFD700]💡 Dica:[/color] [color=#A0A0A0]Configure um min_exp alto para evitar spam de notificações pequenas![/color]\n"
        "[b][color=#4169E1]═══════════════════════════════════════════════[/color][/b]"
    )
    
    HELP_PKC = (
        "\n"
        "[b][color=#DC143C]═══════════════════════════════════════════════[/color][/b]\n"
        "[b][color=#FF69B4]🔒 Ajuda Detalhada - Sistema de Bloqueio de Canal (PKC)[/color][/b]\n"
        "[b][color=#DC143C]═══════════════════════════════════════════════[/color][/b]\n\n"
        "[b][color=#DC143C]!pkc[/color][/b] [color=#A0A0A0]<channel_id> <minutos> <senha>[/color]\n"
        "[color=#A0A0A0]Bloqueia um canal por tempo determinado. Qualquer pessoa que entrar[/color]\n"
        "[color=#A0A0A0]no canal será kickada automaticamente até o tempo expirar.[/color]\n"
        "[color=#A0A0A0]Máximo de 3 canais ativos simultaneamente.[/color]\n"
        "[color=#A0A0A0]Duração máxima: 180 minutos (3 horas).[/color]\n"
        "[color=#90EE90]Exemplo:[/color]\n"
        "  !pkc 5 30 \"senha\"    (bloqueia canal 5 por 30 min)\n\n"
        "[b][color=#8B0000]!cancelpkc[/color][/b]\n"
        "[color=#A0A0A0]Cancela todos os bloqueios de canal ativos imediatamente.[/color]\n"
        "[color=#90EE90]Exemplo:[/color] !cancelpkc\n\n"
        "[b][color=#CD5C5C]!pkclogs[/color][/b] [color=#A0A0A0][nickname/clid][/color]\n"
        "[color=#A0A0A0]Visualiza logs de kicks do sistema PKC.[/color]\n"
        "[color=#A0A0A0]Opcionalmente filtre por nickname ou client ID.[/color]\n"
        "[color=#90EE90]Exemplos:[/color]\n"
        "  !pkclogs           (todos os logs)\n"
        "  !pkclogs john      (logs do jogador john)\n\n"
        "[color=#FF6B6B]⚠️ Importante:[/color]\n"
        "[color=#A0A0A0]• Senha necessária para ativar PKC (peça ao administrador)[/color]\n"
        "[color=#A0A0A0]• Limite de 3 canais simultaneamente[/color]\n"
        "[color=#A0A0A0]• Use !channelids para ver IDs dos canais[/color]\n\n"
        "[color=#FFD700]💡 Dica:[/color] [color=#A0A0A0]Use !pkclogs para ver quem tentou entrar nos canais bloqueados![/color]\n"
        "[b][color=#DC143C]═══════════════════════════════════════════════[/color][/b]"
    )
    
    try:
        # Check for detailed help subcommands FIRST (before general !help)
        if msg.startswith("!help warexp"):
            return HELP_WAREXP
        
        if msg.startswith("!help logger"):
            return HELP_LOGGER
        
        if msg.startswith("!help exp"):
            return HELP_EXP
        
        if msg.startswith("!help channel"):
            return (
                "\n"
                "[b][color=#20B2AA]═══════════════════════════════════════════════[/color][/b]\n"
                "[b][color=#FF69B4]🎯 Ajuda Detalhada - Comandos de Canal[/color][/b]\n"
                "[b][color=#20B2AA]═══════════════════════════════════════════════[/color][/b]\n\n"
                "[b][color=#20B2AA]!channelids[/color][/b]\n"
                "[color=#A0A0A0]Lista todos os canais do servidor com seus IDs.[/color]\n"
                "[color=#A0A0A0]Útil para usar com o comando !pkc.[/color]\n"
                "[color=#90EE90]Exemplo:[/color] !channelids\n\n"
                "[b][color=#FFD700]!bdsm[/color][/b]\n"
                "[color=#A0A0A0]Move você e o bot para o canal Djinns.[/color]\n"
                "[color=#A0A0A0]Comando especial para acesso rápido.[/color]\n"
                "[color=#90EE90]Exemplo:[/color] !bdsm\n\n"
                "[color=#FFD700]💡 Dica:[/color] [color=#A0A0A0]Use !channelids para descobrir o ID do canal antes de usar !pkc![/color]\n"
                "[b][color=#20B2AA]═══════════════════════════════════════════════[/color][/b]"
            )
        
        if msg.startswith("!help pkc"):
            return HELP_PKC
        
        if msg.startswith("!help users"):
            return (
                "\n"
                "[b][color=#32CD32]═══════════════════════════════════════════════[/color][/b]\n"
                "[b][color=#FF69B4]👥 Ajuda Detalhada - Comandos de Usuários[/color][/b]\n"
                "[b][color=#32CD32]═══════════════════════════════════════════════[/color][/b]\n\n"
                "[b][color=#32CD32]!users[/color][/b]\n"
                "[color=#A0A0A0]Lista todos os UIDs únicos com seus nicknames associados.[/color]\n"
                "[color=#A0A0A0]Mostra todo histórico de nomes usados por cada UID.[/color]\n"
                "[color=#90EE90]Exemplo:[/color] !users\n\n"
                "[b][color=#228B22]!users plus[/color][/b]\n"
                "[color=#A0A0A0]Lista apenas usuários que usaram múltiplos nicknames.[/color]\n"
                "[color=#A0A0A0]Útil para identificar alternativas e mudanças de nome.[/color]\n"
                "[color=#90EE90]Exemplo:[/color] !users plus\n\n"
                "[color=#FFD700]💡 Dica:[/color] [color=#A0A0A0]Use !users plus para detectar mudanças de nickname![/color]\n"
                "[b][color=#32CD32]═══════════════════════════════════════════════[/color][/b]"
            )
        
        if msg.startswith("!help mp"):
            return (
                "\n"
                "[b][color=#FF8C00]═══════════════════════════════════════════════[/color][/b]\n"
                "[b][color=#FF69B4]📢 Ajuda Detalhada - Mass Poke[/color][/b]\n"
                "[b][color=#FF8C00]═══════════════════════════════════════════════[/color][/b]\n\n"
                "[b][color=#FF8C00]!mp[/color][/b] [color=#A0A0A0]<mensagem>[/color]\n"
                "[color=#A0A0A0]Envia um poke para todos online no servidor com sua mensagem.[/color]\n"
                "[color=#A0A0A0]Útil quando o x3tbot está offline ou para avisos urgentes.[/color]\n"
                "[color=#90EE90]Exemplos:[/color]\n"
                "  !mp Guerra começou!!\n"
                "  !mp Raid em 10 minutos\n"
                "  !mp Alguém pode ajudar na quest?\n\n"
                "[color=#FF6B6B]⚠️ Aviso:[/color] [color=#A0A0A0]Use com moderação! Todos online receberão o poke.[/color]\n"
                "[b][color=#FF8C00]═══════════════════════════════════════════════[/color][/b]"
            )
        
        if msg.startswith("!help uptime"):
            return (
                "\n"
                "[b][color=#1E90FF]═══════════════════════════════════════════════[/color][/b]\n"
                "[b][color=#FF69B4]⏱️ Ajuda Detalhada - Uptime[/color][/b]\n"
                "[b][color=#1E90FF]═══════════════════════════════════════════════[/color][/b]\n\n"
                "[b][color=#1E90FF]!uptime[/color][/b]\n"
                "[color=#A0A0A0]Mostra há quanto tempo o bot está rodando sem reiniciar.[/color]\n"
                "[color=#A0A0A0]Inclui data/hora de início e duração total.[/color]\n"
                "[color=#90EE90]Exemplo:[/color] !uptime\n\n"
                "[color=#FFD700]💡 Dica:[/color] [color=#A0A0A0]Use para verificar se o bot reiniciou recentemente![/color]\n"
                "[b][color=#1E90FF]═══════════════════════════════════════════════[/color][/b]"
            )
        
        # General help command (checked AFTER all subcommands)
        if msg.startswith("!help"):
            return (
                "\n"
                "[b][color=#FF1493]═════════════════════════════════════════════════[/color][/b]\n"
                "[b][color=#FF69B4]🪲 ROLLABOT - Comandos Disponíveis[/color][/b]\n"
                "[b][color=#FF1493]═════════════════════════════════════════════════[/color][/b]\n\n"
                "[b][color=#FF4500]!help[/color][/b] [color=#A0A0A0][comando][/color] - Mostra esta mensagem ou ajuda detalhada\n"
                "[b][color=#FF8C00]!mp[/color][/b] [color=#A0A0A0]<mensagem>[/color] - Cutucar todos (útil se x3tbot offline)\n"
                "[b][color=#FFD700]!logger[/color][/b] [color=#A0A0A0]<uid/nick/ip> [all][/color] - Buscar log de atividade\n"
                "[b][color=#9ACD32]!lastminuteslogs[/color][/b] [color=#A0A0A0]<minutos>[/color] - Atividade dos últimos N minutos\n"
                "[b][color=#32CD32]!users[/color][/b] - Listar todos UIDs com nicknames\n"
                "[b][color=#228B22]!users plus[/color][/b] - Listar apenas usuários com múltiplos nicks\n"
                "[b][color=#20B2AA]!channelids[/color][/b] - Listar canais com seus IDs\n"
                "[b][color=#00CED1]!registered[/color][/b] - Ver quantos registrados para exp\n"
                "[b][color=#1E90FF]!uptime[/color][/b] - Ver tempo online do bot\n"
                "[b][color=#4169E1]!registerexp[/color][/b] [color=#A0A0A0][min_exp][/color] - Registrar para notificações de exp\n"
                "[b][color=#8A2BE2]!unregisterexp[/color][/b] - Cancelar notificações de exp\n"
                "[b][color=#9932CC]!warexp[/color][/b] - Estatísticas da guerra (Shell vs Ascendant)\n"
                "[b][color=#FF1493]!warexplog[/color][/b] [color=#A0A0A0][dias][/color] - Histórico de guerra (padrão: 30 dias)\n"
                "[b][color=#FF4500]!explog[/color][/b] [color=#A0A0A0][minutos][/color] - Ganhos recentes de exp (padrão: 100)\n"
                "[b][color=#FF6347]!explogger[/color][/b] [color=#A0A0A0]<nome> [all][/color] - Buscar ganhos por nome\n"
                "[b][color=#FF8C00]!showlogs[/color][/b] - Ver últimos 100 avisos/erros\n"
                "[b][color=#FFD700]!bdsm[/color][/b] - Mover você e o bot para canal Djinns\n"
                "[b][color=#DC143C]!pkc[/color][/b] [color=#A0A0A0]<canal> <minutos> <senha>[/color] - Bloquear canal (máx 3)\n"
                "[b][color=#8B0000]!cancelpkc[/color][/b] - Cancelar todos bloqueios de canal\n"
                "[b][color=#CD5C5C]!pkclogs[/color][/b] [color=#A0A0A0][nick/clid][/color] - Ver logs de kicks PKC\n"
                "\n"
                "[color=#FFD700]💡 Ajuda Detalhada:[/color] [color=#A0A0A0]Digite [/color][b]!help <comando>[/b]\n"
                "[color=#A0A0A0]Exemplos: !help warexp | !help logger | !help exp | !help pkc[/color]\n"
                "\n"
                "[i]Obs: [color=#A0A0A0]Obrigado Pedrin pelas apis que eu robei na cara dura. PATROCINIO:[url]https://bit.ly/3Mt6fxE[/url][/color][/i]\n"
                "[color=#8B8B8B]─────────────────────────────────────────────────[/color]"

                # "!registerfriendlyexp - Register for friendly guild exp notifications\n"  # Commented out
                # "!unregisterfriendlyexp - Unregister from friendly guild exp notifications\n"  # Commented out
            )





        # Mass poke command
        if msg.startswith("!mp"):
            bot.masspoke(f"{nickname} te cutucou: {msg[4:]}")
            return "\n[b][color=#4ECDC4]📢 Cutucando todos os clientes...[/color][/b]"
        
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
            args = msg[7:].strip()
            if not args:
                return HELP_LOGGER
            
            # Check if "all" is in the arguments
            parts = args.split()
            show_all = False
            if len(parts) > 1 and parts[-1].lower() == "all":
                show_all = True
                search_term = " ".join(parts[:-1])
            else:
                search_term = args
            
            # Call with max_results=None to show all, or default 50
            max_results = None if show_all else 50
            return "\n" + search_activity_log(search_term, max_results=max_results)
        
        # Get recent logs by minutes
        if msg.startswith("!lastminuteslogs"):
            try:
                minutes_str = msg[16:].strip()
                if not minutes_str:
                    return HELP_LOGGER
                
                minutes = int(minutes_str)
                if minutes <= 0:
                    return "\n[color=#FF6B6B]❌ Erro: Minutos deve ser um número positivo.[/color]\n" + HELP_LOGGER
                if minutes > 1440:  # 24 hours
                    return "\n[color=#FF6B6B]❌ Erro: Máximo de 1440 minutos (24 horas) permitido.[/color]\n" + HELP_LOGGER
                
                return "\n" + get_recent_logs(minutes)
            except ValueError:
                return HELP_LOGGER
        
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
        if msg.startswith("!pkclogs"):
            try:
                # Parse optional search term: !pkclogs <nickname/clid>
                parts = msg.split(maxsplit=1)
                
                if len(parts) > 1:
                    search_term = parts[1].strip()
                    return "\n" + get_pkc_logs(search_term=search_term)
                else:
                    return "\n" + get_pkc_logs()
                    
            except Exception as e:
                logger.error(f"Error in !pkclogs command: {e}")
                return f"\n[color=#FF0000]Error: {str(e)}[/color]"
        
        # Periodic kick channel command
        if msg.startswith("!pkc"):
            try:
                # Parse command: !pkc <channel_id> <duration_minutes> <password>
                # Use shlex to handle quoted password
                import shlex
                parts = shlex.split(msg)
                
                if len(parts) < 4:
                    return HELP_PKC
                
                channel_id = parts[1]
                duration_minutes = int(parts[2])
                password = parts[3]
                
                # Hardcoded password check
                if password != "capivara69":
                    return "\n[color=#FF6B6B]Senha inválida.[/color]"
                
                # Validate parameters
                if duration_minutes < 1:
                    return "\n[color=#FF6B6B]❌ Erro: Duração deve ser pelo menos 1 minuto.[/color]\n" + HELP_PKC
                
                if duration_minutes > 180:
                    return "\n[color=#FF6B6B]❌ Erro: Duração máxima é 180 minutos (3 horas).[/color]\n" + HELP_PKC
                
                # Check if max concurrent channels reached (safe to read, only worker modifies)
                if len(bot.active_pkc_channels) >= 3:
                    return "\n[color=#FF6B6B]Máximo de canais monitorados ativo (3/3). Aguarde um ser concluído.[/color]"
                
                # Check if channel already being monitored (safe to read, only worker modifies)
                if channel_id in bot.active_pkc_channels:
                    return f"\n[color=#FF6B6B]Canal {channel_id} já está sendo monitorado.[/color]"
                
                # Create thread info
                thread_id = f"pkc_{channel_id}_{int(time.time())}"
                thread = threading.Thread(
                    target=periodic_kick_channel,
                    args=(bot, channel_id, duration_minutes, thread_id),
                    daemon=True
                )
                
                # Add to active channels (directly, will be accessed by worker thread only)
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
                
                return f"\n[b][color=#4ECDC4]🔒 Canal {channel_id} bloqueado por {duration_minutes} minutos.[/color][/b]\n[color=#A0A0A0]Monitores ativos: {len(bot.active_pkc_channels)}/3[/color]"
                    
            except ValueError:
                return HELP_PKC
            except Exception as e:
                logger.error(f"Error in !pkc command: {e}")
                return f"\n[color=#FF0000]Erro: {str(e)}[/color]"
        
        # Cancel all PKC operations
        if msg.startswith("!cancelpkc"):
            try:
                # Queue a request to cancel all PKC channels
                # The worker thread will handle this to avoid race conditions
                bot.command_queue.put({
                    'type': 'pkc_cancel_all'
                })
                
                # Give the worker thread a moment to process
                time.sleep(0.1)
                
                # Check if there were any channels (this is safe to read after worker processes)
                if not bot.active_pkc_channels:
                    return "\n[color=#FF6B6B]Nenhum bloqueio de canal ativo para cancelar.[/color]"
                
                return "\n[b][color=#4ECDC4]🔓 Todos os bloqueios de canal foram cancelados.[/color][/b]"
                
            except Exception as e:
                logger.error(f"Error in !cancelpkc command: {e}")
                return f"\n[color=#FF0000]Erro: {str(e)}[/color]"
        
        # Get PKC event kick logs
       
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
                    return "\n[color=#FF6B6B]Nenhum histórico de comandos disponível.[/color]"
                
                # Reverse to show most recent first
                history.reverse()
                
                message = "[b][color=#FFD700]═══ Histórico de Comandos ═══[/color][/b]\n"
                message += "[color=#505050]" + "═" * 60 + "[/color]\n\n"
                
                for timestamp, nickname, command in history:
                    message += f"[color=#A0A0A0]{timestamp}[/color] [color=#505050]-[/color] "
                    message += f"[color=#4ECDC4]{nickname}[/color] [color=#505050]-[/color] "
                    message += f"[color=#FFD700]{command}[/color]\n"
                
                message += "\n[color=#505050]" + "═" * 60 + "[/color]"
                return "\n" + message
            except Exception as e:
                logger.error(f"Error retrieving command history: {e}")
                return "\n[color=#FF0000]Erro ao recuperar histórico de comandos.[/color]"
        
        # Register for guild exp notifications
        if msg.startswith("!registerexp"):
            # Get user UID from reference data (avoid API calls from event loop)
            try:
                # Parse optional min_exp parameter: !registerexp <min_exp>
                parts = msg.split()
                min_exp = 0
                
                if len(parts) > 1:
                    try:
                        min_exp = int(parts[1])
                        if min_exp < 0:
                            return "\n[color=#FF6B6B]❌ Erro: Exp mínima deve ser 0 ou maior.[/color]\n" + HELP_EXP
                    except ValueError:
                        return HELP_EXP
                
                logger.debug(f"Processing registerexp for nickname: {nickname} with min_exp: {min_exp}")
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
                    logger.debug(f"Registering user UID: {user_uid} for exp notifications with min_exp: {min_exp}")
                    return "\n" + register_exp_user(user_uid, min_exp)
                else:
                    return "\n[color=#FF6B6B]Não foi possível encontrar seu UID. Aguarde um minuto para os dados atualizarem e tente novamente.[/color]"
            except Exception as e:
                logger.error(f"Error in registerexp command: {e}")
                return "\n[color=#FF0000]Erro ao registrar. Tente novamente.[/color]"
        
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
                    return "\n[color=#FF6B6B]Não foi possível encontrar seu UID. Aguarde um minuto para os dados atualizarem e tente novamente.[/color]"
            except Exception as e:
                logger.error(f"Error in unregisterexp command: {e}")
                return "\n[color=#FF0000]Erro ao desregistrar. Tente novamente.[/color]"
        
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
                        return "\n[color=#FF6B6B]❌ Erro: Dias devem estar entre 1 e 365.[/color]\n" + HELP_WAREXP
                
                return "\n" + get_war_exp_log(days)
            except ValueError:
                return HELP_WAREXP
            except Exception as e:
                logger.error(f"Error in warexplog command: {e}")
                return "\n[color=#FF0000]Erro ao recuperar log de exp de guerra.[/color]"
        
        if msg.startswith("!warexp"):
            try:
                if not hasattr(bot, 'war_stats_collector'):
                    return "\n[color=#FF6B6B]Coletor de estatísticas de guerra não disponível.[/color]"
                
                stats_data, last_update = bot.war_stats_collector.get_stats()
                return "\n" + format_war_stats(stats_data, last_update)
            except Exception as e:
                logger.error(f"Error in warexp command: {e}")
                return "\n[color=#FF0000]Erro ao recuperar estatísticas de guerra. Tente novamente.[/color]"
        
        # Exp deltas log command
        if msg.startswith("!explogger"):
            args = msg[10:].strip()
            if not args:
                return HELP_EXP
            
            # Check if "all" is in the arguments
            parts = args.split()
            show_all = False
            if len(parts) > 1 and parts[-1].lower() == "all":
                show_all = True
                search_term = " ".join(parts[:-1])
            else:
                search_term = args
            
            # Call with max_results=None to show all, or default 50
            max_results = None if show_all else 50
            return "\n" + search_exp_log(search_term, max_results=max_results)
        
        if msg.startswith("!explog"):
            try:
                parts = msg.split()
                
                # Default to 100 entries if no parameter
                if len(parts) < 2:
                    return "\n" + get_exp_log(minutes=None, entries=100)
                else:
                    minutes = int(parts[1])
                    if minutes < 1 or minutes > 1440:  # Max 24 hours
                        return "\n[color=#FF6B6B]❌ Erro: Minutos devem estar entre 1 e 1440.[/color]\n" + HELP_EXP
                    
                    return "\n" + get_exp_log(minutes=minutes)
            except ValueError:
                return HELP_EXP
            except Exception as e:
                logger.error(f"Error in explog command: {e}")
                return "\n[color=#FF0000]Erro ao recuperar log de exp.[/color]"
        
        # Search exp deltas by name
        
        # Go home command - move user and bot to Djinns channel
        if msg.startswith("!bdsm"):
            try:
                if clid is None:
                    return "\n[color=#FF6B6B]ID do cliente não disponível.[/color]"
                
                # Get bot's own client ID
                try:
                    bdsm_text_async(nickname, bot, clid)
                    whoami = bot.worker_conn.whoami().parsed[0]
                    bot_clid = whoami.get('clid', '')
                except Exception as e:
                    logger.error(f"Error getting bot client ID: {e}")
                    return "\n[color=#FF0000]Erro: Não foi possível obter ID do bot.[/color]"
                
                # Move both user and bot to Djinns
                success = bot.move_to_djinns(clid, bot_clid)
                
                if success:
                    # Start async API call that will send response when ready
                    
                    return "\n[b][color=#4ECDC4]Hmmmm...[/color][/b]"
                else:
                    return "\n[color=#FF6B6B]Falha ao mover para o canal Djinns. Canal pode não existir.[/color]"
                    
            except Exception as e:
                logger.error(f"Error in bdsm command: {e}")
                return "\n[color=#FF0000]Erro ao executar comando bdsm.[/color]"
        
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
                    return "\n[color=#FF6B6B]Manipulador de logs não disponível.[/color]"
                
                logs = bot.log_handler.get_logs(100)
                
                if not logs:
                    return "\n[color=#A0A0A0]Nenhum aviso ou erro registrado ainda.[/color]"
                
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
                message = f"[b][color=#FFD700]═══ Logs do Bot (Últimas {len(logs)} Entradas) ═══[/color][/b]\n"
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
                return "\n[color=#FF0000]Erro ao recuperar logs.[/color]"
    except Exception as e:
        logger.error(f"Error processing command: {e}")
        return f"\n[color=#FF0000]QUEBREI: {e}.[/color]"
    
    # Unknown command




    t=get_txt()
    if isinstance(t, str) and t.strip():
        t=f"\n[color=#FF6B6B]{t.strip()}[/color]"
    else:
        t=""
    default_response = f"\n[color=#A0A0A0]Esse não é o x3tbot. Digite[/color] [b][color=#4ECDC4]!help[/color][/b] [color=#A0A0A0]para ver os comandos disponíveis[/color] {t}"
    
    return str(default_response)
