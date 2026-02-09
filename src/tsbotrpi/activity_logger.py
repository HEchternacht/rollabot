import csv
import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)


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
