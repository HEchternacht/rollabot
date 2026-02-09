import csv
import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class ReferenceDataManager:
    """Manages reference data for clients and channels."""
    
    def __init__(self, clients_csv: str, channels_csv: str):
        """
        Initialize reference data manager.
        
        Args:
            clients_csv: Path to clients reference CSV
            channels_csv: Path to channels reference CSV
        """
        self.clients_csv = clients_csv
        self.channels_csv = channels_csv
        self.client_map: Dict[str, dict] = {}  # clid -> client info
        self.channel_map: Dict[str, str] = {}  # cid -> channel name
        
        # Load existing data
        self._load_clients()
        self._load_channels()
    
    def _load_clients(self):
        """Load existing client data from CSV."""
        if not os.path.exists(self.clients_csv):
            return
        
        try:
            with open(self.clients_csv, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    clid = row.get('clid', '')
                    if clid:
                        self.client_map[clid] = {
                            'nickname': row.get('nickname', ''),
                            'uid': row.get('uid', ''),
                            'ip': row.get('ip', '')
                        }
            logger.info(f"Loaded {len(self.client_map)} clients from reference data")
        except Exception as e:
            logger.error(f"Failed to load client reference data: {e}")
    
    def _load_channels(self):
        """Load existing channel data from CSV."""
        if not os.path.exists(self.channels_csv):
            return
        
        try:
            with open(self.channels_csv, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    cid = row.get('cid', '')
                    if cid:
                        self.channel_map[cid] = row.get('channel_name', '')
            logger.info(f"Loaded {len(self.channel_map)} channels from reference data")
        except Exception as e:
            logger.error(f"Failed to load channel reference data: {e}")
    
    def update_clients(self, clients: list):
        """
        Update client reference data.
        
        Args:
            clients: List of client dicts from clientlist()
        """
        try:
            # Update in-memory map
            for client in clients:
                clid = client.get('clid', '')
                if clid:
                    self.client_map[clid] = {
                        'nickname': client.get('client_nickname', ''),
                        'uid': client.get('client_unique_identifier', ''),
                        'ip': client.get('connection_client_ip', '')
                    }
            
            # Write to CSV
            file_exists = os.path.exists(self.clients_csv)
            with open(self.clients_csv, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['timestamp', 'clid', 'nickname', 'uid', 'ip'])
                
                timestamp = datetime.now().isoformat()
                for clid, info in self.client_map.items():
                    writer.writerow([
                        timestamp,
                        clid,
                        info.get('nickname', ''),
                        info.get('uid', ''),
                        info.get('ip', '')
                    ])
            
            logger.debug(f"Updated {len(clients)} clients in reference data")
            
        except Exception as e:
            logger.error(f"Failed to update client reference data: {e}")
    
    def update_channels(self, channels: list):
        """
        Update channel reference data.
        
        Args:
            channels: List of channel dicts from channellist()
        """
        try:
            # Update in-memory map
            for channel in channels:
                cid = channel.get('cid', '')
                if cid:
                    self.channel_map[cid] = channel.get('channel_name', '')
            
            # Write to CSV
            with open(self.channels_csv, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['timestamp', 'cid', 'channel_name'])
                
                timestamp = datetime.now().isoformat()
                for cid, name in self.channel_map.items():
                    writer.writerow([timestamp, cid, name])
            
            logger.debug(f"Updated {len(channels)} channels in reference data")
            
        except Exception as e:
            logger.error(f"Failed to update channel reference data: {e}")
    
    def get_client_info(self, clid: str) -> dict:
        """Get client info by clid."""
        return self.client_map.get(clid, {'nickname': 'Unknown', 'uid': '', 'ip': ''})
    
    def get_channel_name(self, cid: str) -> str:
        """Get channel name by cid."""
        return self.channel_map.get(cid, f'Channel {cid}')


class UsersSeenTracker:
    """Tracks unique users seen on the server."""
    
    def __init__(self, csv_path: str):
        """
        Initialize users seen tracker.
        
        Args:
            csv_path: Path to users_seen.csv
        """
        self.csv_path = csv_path
        self.seen_users = set()  # Set of (uid, nickname, ip) tuples
        
        # Load existing users
        self._load_existing()
    
    def _load_existing(self):
        """Load existing users from CSV."""
        if not os.path.exists(self.csv_path):
            return
        
        try:
            with open(self.csv_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader, None)  # Skip header
                for row in reader:
                    if len(row) >= 3:
                        self.seen_users.add((row[0], row[1], row[2]))
            logger.info(f"Loaded {len(self.seen_users)} unique users from users_seen.csv")
        except Exception as e:
            logger.error(f"Failed to load users_seen.csv: {e}")
    
    def add_users(self, clients: list):
        """
        Add users from client list.
        
        Args:
            clients: List of client dicts
        """
        try:
            new_users = []
            
            for client in clients:
                uid = client.get('client_unique_identifier', '')
                nickname = client.get('client_nickname', '')
                ip = client.get('connection_client_ip', '')
                
                # Skip if any field is empty
                if not uid or not nickname or not ip:
                    continue
                
                user_tuple = (uid, nickname, ip)
                if user_tuple not in self.seen_users:
                    self.seen_users.add(user_tuple)
                    new_users.append(user_tuple)
            
            # Append new users to CSV
            if new_users:
                file_exists = os.path.exists(self.csv_path)
                with open(self.csv_path, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    
                    if not file_exists:
                        writer.writerow(['UID', 'NICKNAME', 'IP'])
                    
                    for uid, nickname, ip in new_users:
                        writer.writerow([uid, nickname, ip])
                
                logger.info(f"Added {len(new_users)} new users to users_seen.csv")
            
        except Exception as e:
            logger.error(f"Failed to add users to users_seen.csv: {e}")


class HumanReadableActivityLogger:
    """Logs activities in human-readable format."""
    
    def __init__(self, csv_path: str, reference_manager: ReferenceDataManager):
        """
        Initialize human-readable activity logger.
        
        Args:
            csv_path: Path to activity log CSV
            reference_manager: Reference data manager for lookups
        """
        self.csv_path = csv_path
        self.reference_manager = reference_manager
        self.file_handle = None
        self.csv_writer = None
        
        try:
            file_exists = os.path.exists(csv_path)
            self.file_handle = open(csv_path, 'a', newline='', encoding='utf-8')
            self.csv_writer = csv.writer(self.file_handle)
            
            if not file_exists:
                self.csv_writer.writerow(['UID', 'TIMESTAMP', 'EVENT'])
                self.file_handle.flush()
                logger.info(f"Created human-readable activity log: {csv_path}")
            else:
                logger.info(f"Opened human-readable activity log: {csv_path}")
                
        except Exception as e:
            logger.error(f"Failed to open activity log: {e}")
            self.file_handle = None
            self.csv_writer = None
    
    def log_event(self, clid: str, event_type: str, event_data: dict):
        """
        Log an event in human-readable format.
        
        Args:
            clid: Client ID
            event_type: Type of event
            event_data: Event data dict
        """
        if not self.csv_writer:
            return
        
        try:
            # Get client info
            client_info = self.reference_manager.get_client_info(clid)
            uid = client_info.get('uid', '')
            nickname = client_info.get('nickname', 'Unknown')
            
            # Generate human-readable event description
            event_desc = self._format_event(event_type, nickname, event_data)
            
            # Log it
            timestamp = datetime.now().strftime('%d/%m/%Y-%H:%M:%S')
            self.csv_writer.writerow([uid, timestamp, event_desc])
            self.file_handle.flush()
            
            logger.debug(f"Logged: {uid} - {event_desc}")
            
        except Exception as e:
            logger.error(f"Failed to log event: {e}")
    
    def _format_event(self, event_type: str, nickname: str, data: dict) -> str:
        """Format event into human-readable description."""
        
        if event_type == 'cliententerview':
            return f"Connected to server"
        
        elif event_type == 'clientleftview':
            reason = data.get('reasonmsg', '')
            if reason:
                return f"Disconnected: {reason}"
            return "Disconnected from server"
        
        elif event_type == 'clientmoved':
            from_cid = data.get('cfid', '')
            to_cid = data.get('ctid', '')
            from_name = self.reference_manager.get_channel_name(from_cid)
            to_name = self.reference_manager.get_channel_name(to_cid)
            return f"Moved from channel {from_name} to {to_name}"
        
        elif event_type == 'clientupdated':
            # Check for nickname change
            if 'client_nickname' in data:
                old_nickname = data.get('old_nickname', '')
                new_nickname = data.get('client_nickname', '')
                if old_nickname and new_nickname and old_nickname != new_nickname:
                    return f"Changed nickname from {old_nickname} to {new_nickname}"
            
            # Check for mute status
            if 'client_input_muted' in data:
                muted = data.get('client_input_muted') == '1'
                if muted:
                    return "Muted input microphone"
                else:
                    return "Unmuted input microphone"
            
            if 'client_output_muted' in data:
                muted = data.get('client_output_muted') == '1'
                if muted:
                    return "Muted output speakers"
                else:
                    return "Unmuted output speakers"
            
            # Generic update
            return "Updated client properties"
        
        else:
            return f"Event: {event_type}"
    
    def cleanup_old_entries(self, days: int = 30):
        """Remove log entries older than specified days."""
        if not os.path.exists(self.csv_path):
            return
        
        try:
            cutoff_date = datetime.now() - timedelta(days=days)
            temp_path = f"{self.csv_path}.tmp"
            
            cleaned_count = 0
            kept_count = 0
            
            with open(self.csv_path, 'r', newline='', encoding='utf-8') as infile:
                reader = csv.reader(infile)
                with open(temp_path, 'w', newline='', encoding='utf-8') as outfile:
                    writer = csv.writer(outfile)
                    
                    # Copy header
                    header = next(reader, None)
                    if header:
                        writer.writerow(header)
                    
                    # Filter rows by date
                    for row in reader:
                        if len(row) < 2:
                            continue
                        
                        try:
                            # Parse timestamp in DD/MM/YYYY-HH:MM:SS format
                            timestamp_str = row[1]
                            row_date = datetime.strptime(timestamp_str, '%d/%m/%Y-%H:%M:%S')
                            if row_date >= cutoff_date:
                                writer.writerow(row)
                                kept_count += 1
                            else:
                                cleaned_count += 1
                        except (ValueError, IndexError):
                            # Keep rows with invalid dates
                            writer.writerow(row)
                            kept_count += 1
            
            # Replace original file
            os.replace(temp_path, self.csv_path)
            logger.info(f"Cleaned activity log: removed {cleaned_count} old entries, kept {kept_count}")
            
            # Reopen file handle
            if self.file_handle:
                self.file_handle.close()
            self.file_handle = open(self.csv_path, 'a', newline='', encoding='utf-8')
            self.csv_writer = csv.writer(self.file_handle)
            
        except Exception as e:
            logger.error(f"Failed to cleanup old log entries: {e}")
    
    def close(self):
        """Close the log file."""
        if self.file_handle:
            try:
                self.file_handle.close()
                logger.info("Human-readable activity logger closed")
            except Exception as e:
                logger.error(f"Error closing activity log: {e}")
            finally:
                self.file_handle = None
                self.csv_writer = None


# Keep old classes for backward compatibility
class ActivityLogger:
    """Logs TeamSpeak user activities to CSV file."""

    def __init__(self, csv_path: str):
        """
        Initialize activity logger.
        
        Args:
            csv_path: Path to the CSV file for logging activities
        """
        self.csv_path = csv_path
        self.file_handle = None
        self.csv_writer = None
        
        try:
            # Create file if it doesn't exist and write header
            file_exists = os.path.exists(csv_path)
            self.file_handle = open(csv_path, 'a', newline='', encoding='utf-8')
            self.csv_writer = csv.writer(self.file_handle)
            
            if not file_exists:
                # Write header
                self.csv_writer.writerow([
                    'timestamp', 'event_type', 'clid', 'nickname', 'uid', 'ip', 'details'
                ])
                self.file_handle.flush()
                logger.info(f"Created new activity log: {csv_path}")
            else:
                logger.info(f"Opened existing activity log: {csv_path}")
                
        except Exception as e:
            logger.error(f"Failed to open activity log file: {e}")
            self.file_handle = None
            self.csv_writer = None

    def cleanup_old_entries(self, days: int = 30):
        """
        Remove log entries older than specified days.
        
        Args:
            days: Number of days to keep (default: 30)
        """
        if not os.path.exists(self.csv_path):
            return
        
        try:
            cutoff_date = datetime.now() - timedelta(days=days)
            temp_path = f"{self.csv_path}.tmp"
            
            cleaned_count = 0
            kept_count = 0
            
            with open(self.csv_path, 'r', newline='', encoding='utf-8') as infile:
                reader = csv.reader(infile)
                with open(temp_path, 'w', newline='', encoding='utf-8') as outfile:
                    writer = csv.writer(outfile)
                    
                    # Copy header
                    header = next(reader, None)
                    if header:
                        writer.writerow(header)
                    
                    # Filter rows by date
                    for row in reader:
                        if len(row) < 1:
                            continue
                        
                        try:
                            row_date = datetime.fromisoformat(row[0])
                            if row_date >= cutoff_date:
                                writer.writerow(row)
                                kept_count += 1
                            else:
                                cleaned_count += 1
                        except (ValueError, IndexError):
                            # Keep rows with invalid dates
                            writer.writerow(row)
                            kept_count += 1
            
            # Replace original file
            os.replace(temp_path, self.csv_path)
            logger.info(f"Cleaned activity log: removed {cleaned_count} old entries, kept {kept_count}")
            
            # Reopen file handle
            if self.file_handle:
                self.file_handle.close()
            self.file_handle = open(self.csv_path, 'a', newline='', encoding='utf-8')
            self.csv_writer = csv.writer(self.file_handle)
            
        except Exception as e:
            logger.error(f"Failed to cleanup old log entries: {e}")
            # Continue operation even if cleanup fails

    def log_event(self, event_type: str, clid: str, client_info: dict, details: dict):
        """
        Log a user activity event.
        
        Args:
            event_type: Type of event (e.g., 'cliententerview', 'clientmoved')
            clid: Client ID
            client_info: Dict with 'nickname', 'uid', 'ip' keys
            details: Additional event details to store as JSON
        """
        if not self.csv_writer:
            logger.warning("Activity logger not initialized, skipping log")
            return
        
        try:
            timestamp = datetime.now().isoformat()
            nickname = client_info.get('nickname', '')
            uid = client_info.get('uid', '')
            ip = client_info.get('ip', '')
            details_json = json.dumps(details, ensure_ascii=False)
            
            self.csv_writer.writerow([
                timestamp, event_type, clid, nickname, uid, ip, details_json
            ])
            self.file_handle.flush()
            
            logger.debug(f"Logged event: {event_type} for clid={clid} ({nickname})")
            
        except Exception as e:
            logger.error(f"Failed to log event {event_type}: {e}")

    def close(self):
        """Close the log file."""
        if self.file_handle:
            try:
                self.file_handle.close()
                logger.info("Activity logger closed")
            except Exception as e:
                logger.error(f"Error closing activity log: {e}")
            finally:
                self.file_handle = None
                self.csv_writer = None


class ClientListLogger:
    """Logs client list snapshots with timestamps."""

    @staticmethod
    def log_clients(csv_path: str, clients: list):
        """
        Append current client list to CSV.
        
        Args:
            csv_path: Path to the clients log CSV
            clients: List of client dicts with clid, nickname, uid, ip
        """
        try:
            file_exists = os.path.exists(csv_path)
            
            with open(csv_path, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                
                if not file_exists:
                    # Write header
                    writer.writerow(['timestamp', 'clid', 'nickname', 'uid', 'ip'])
                
                timestamp = datetime.now().isoformat()
                for client in clients:
                    writer.writerow([
                        timestamp,
                        client.get('clid', ''),
                        client.get('nickname', ''),
                        client.get('uid', ''),
                        client.get('ip', '')
                    ])
            
            logger.info(f"Logged {len(clients)} clients to {csv_path}")
            
        except Exception as e:
            logger.error(f"Failed to log client list: {e}")
