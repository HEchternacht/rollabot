import logging
import time
import threading
import os
import ts3
from datetime import datetime
from urllib.parse import quote as encodeURIComponent
import requests
from queue import Queue

from .commands import process_command
from .activity_logger import (
    ActivityLogger, 
    ClientListLogger, 
    ReferenceDataManager, 
    UsersSeenTracker, 
    HumanReadableActivityLogger
)

logger = logging.getLogger(__name__)


class TS3Bot:
    """Simplified TeamSpeak bot based on AutoanswerScheduler pattern."""

    def __init__(self, host: str, api_key: str, server_address: str = "", nickname: str = "Rollabot", process_manager=None):
        self.host = host
        self.api_key = api_key
        self.server_address = server_address
        self.nickname = nickname
        self.process_manager = process_manager
        self.conn = None  # Main connection for commands, keepalive, reference data
        self.event_conn = None  # Dedicated connection for event loop
        self.worker_conn = None  # Dedicated connection for worker thread responses
        self._event_thread = None
        self._reference_thread = None
        self._worker_thread = None
        self._running = False
        self.command_queue = Queue()  # FIFO queue for command processing
        self.client_map = {}  # Maps clid -> {nickname, uid, ip}
        self.activity_logger = None
        self.clients_logger_initialized = False
        
        # New components
        self.reference_manager = None
        self.users_seen_tracker = None
        self.human_readable_logger = None
        
        # Duplicate event prevention
        self.last_event = None
        self.last_event_timestamp = 0
        
        # Guild exp monitoring
        self.last_guild_refresh_ts = None
        # self.last_friendly_guild_refresh_ts = None  # Commented out - not needed anymore

    def _ensure_server_connection(self, conn=None, conn_name="connection"):
        """Check if connected to server and reconnect if needed."""
        if conn is None:
            conn = self.conn
        
        if not conn or not self.server_address:
            return
        
        try:
            # Check current connection status
            whoami = conn.whoami().parsed[0]
            client_type = whoami.get('client_type', '')
            
            # If client_type is 1, we're connected to a server
            if client_type == '1':
                return  # Already connected
            
            # Not connected to server, try to connect
            logger.info(f"{conn_name} not connected to server, attempting to connect...")
            conn.send(f"connect address={self.server_address} nickname={self.nickname}")
            logger.info(f"{conn_name} connected to server %s as %s", self.server_address, self.nickname)
            
        except Exception as e:
            # Try to connect anyway
            try:
                conn.send(f"connect address={self.server_address} nickname={self.nickname}")
                logger.info(f"{conn_name} connected to server %s as %s", self.server_address, self.nickname)
            except Exception as e2:
                # Ignore "already connected" error (id 1796)
                if "1796" not in str(e2):
                    logger.debug(f"{conn_name} server connection check/reconnect failed: {e2}")

    def setup_connection(self):
        """Setup TS3 ClientQuery connection for commands and operations."""
        conn = ts3.query.TS3ClientConnection(self.host)
        conn.auth(apikey=self.api_key)
        conn.use()
        
        # Connect to server if address is configured
        if self.server_address:
            try:
                conn.send(f"connect address={self.server_address} nickname={self.nickname}")
                #logger.info("Connected to server %s as %s", self.server_address, self.nickname)
            except Exception as e:
                # Ignore "already connected" error (id 1796)
                #if "1796" in str(e) or "currently not possible" in str(e).lower():
                #    logger.debug("Already connected to server")
                #else:
                #    logger.warning("Connect command failed: %s", e)
                pass
        
        #logger.info("Connected to ClientQuery at %s", self.host)
        
        # Initialize logging components on first successful connection
        if not self.activity_logger:
            try:
                log_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                
                # Old activity logger (kept for backward compatibility)
                activity_log_path = os.path.join(log_dir, 'activities_log.csv')
                self.activity_logger = ActivityLogger(activity_log_path)
                self.activity_logger.cleanup_old_entries(days=30)
                
                # Reference data manager
                clients_ref_path = os.path.join(log_dir, 'clients_reference.csv')
                channels_ref_path = os.path.join(log_dir, 'channels_reference.csv')
                self.reference_manager = ReferenceDataManager(clients_ref_path, channels_ref_path)
                
                # Users seen tracker
                users_seen_path = os.path.join(log_dir, 'users_seen.csv')
                self.users_seen_tracker = UsersSeenTracker(users_seen_path)
                
                # Human-readable activity logger
                human_log_path = os.path.join(log_dir, 'activity_log_readable.csv')
                self.human_readable_logger = HumanReadableActivityLogger(
                    human_log_path, 
                    self.reference_manager
                )
                self.human_readable_logger.cleanup_old_entries(days=30)
                
                logger.info("All logging components initialized")
            except Exception as e:
                logger.error(f"Failed to initialize logging components: {e}")
        
        # Fetch and log current client list on startup
        if not self.clients_logger_initialized:
            self._fetch_and_log_clientlist(conn)
            self._fetch_and_update_channels(conn)
            self.clients_logger_initialized = True
        
        return conn

    def setup_worker_connection(self):
        """Setup dedicated TS3 ClientQuery connection for worker thread."""
        conn = ts3.query.TS3ClientConnection(self.host)
        conn.auth(apikey=self.api_key)
        conn.use()
        
        # Connect to server if address is configured
        if self.server_address:
            try:
                conn.send(f"connect address={self.server_address} nickname={self.nickname}")
            except Exception as e:
                # Ignore "already connected" error
                pass
        
        return conn

    def setup_event_connection(self):
        """Setup dedicated TS3 ClientQuery connection for event loop."""
        conn = ts3.query.TS3ClientConnection(self.host)
        conn.auth(apikey=self.api_key)
        conn.use()
        # Register for ALL notifications
        conn.clientnotifyregister(event="any", schandlerid=1)
        
        # Connect to server if address is configured
        if self.server_address:
            try:
                conn.send(f"connect address={self.server_address} nickname={self.nickname}")
            except Exception as e:
                # Ignore "already connected" error
                pass
        
        return conn

    def _is_connection_refused(self, exc):
        """Check if error is connection refused or address not found."""
        err = str(exc).lower()
        return (
            "refused" in err or 
            "10061" in err or 
            "111" in err or
            "connection refused" in err or
            "address" in err or
            "network" in err or
            "name or service not known" in err or
            "[errno -2]" in err or
            "nodename nor servname" in err
        )

    def _reconnect(self, error=None):
        """Reconnect and start/restart TS client only if connection refused."""
        self.conn = None
        
        # Check if TS client process is running
        ts_is_running = False
        if self.process_manager:
            pid = self.process_manager._read_pid()
            if pid and self.process_manager._is_running(pid):
                ts_is_running = True
                logger.info("TS client process is running (PID %s), attempting reconnection", pid)
        
        # If TS is running, try reconnecting multiple times before restarting
        if ts_is_running:
            for attempt in range(1, 4):  # 3 attempts
                try:
                    logger.info("Reconnection attempt %d/3...", attempt)
                    self.conn = self.setup_connection()
                    logger.info("Reconnected successfully on attempt %d", attempt)
                    return
                except Exception as e:
                    logger.error("Reconnection attempt %d failed: %s", attempt, e)
                    if attempt < 3:
                        logger.info("Waiting 20s before next attempt...")
                        time.sleep(20)
            
            # All reconnection attempts failed
            logger.warning("All 3 reconnection attempts failed, will restart TS client")
        
        # Try to connect first (if TS wasn't running or reconnection attempts failed)
        try:
            self.conn = self.setup_connection()
            logger.info("Reconnected successfully")
            return
        except Exception as e:
            logger.error("Connection failed: %s", e)
            
            # Only start/restart TS client if connection refused or address not found
            if self._is_connection_refused(e) and self.process_manager:
                logger.warning("Connection refused/unavailable - restarting TS client")
                restarted = self.process_manager.restart()
                
                # Wait longer if we actually restarted (box64 takes ~60s)
                # Wait less if restart was skipped due to cooldown
                wait_time = 60 if restarted else 5
                logger.info("Waiting %ds for TS client...", wait_time)
                time.sleep(wait_time)
                
                # Try connecting again
                try:
                    self.conn = self.setup_connection()
                    logger.info("Connected after restarting TS client")
                except Exception as e2:
                    logger.error("Still cannot connect: %s", e2)
                    time.sleep(2)
                    time.sleep(2)

    def get_xbot(self):
        """Find x3tBot Auroria client."""
        clients = self.conn.clientlist().parsed
        for client in clients:
            if "x3tBot Auroria" in client.get("client_nickname", ""):
                return client
        return None

    def add_hunted(self, target):
        """Add target to hunted list via x3tBot."""
        xbot = self.get_xbot()
        if not xbot:
            return "x3tBot not found"
        
        self.conn.sendtextmessage(
            targetmode=1, target=xbot["clid"], msg=f"!hunted add {target}"
        )
        
        # Wait for responses
        for _ in range(10):
            try:
                event = self.conn.wait_for_event(timeout=1)
                if event.parsed:
                    logger.info("xBot response: %s", event.parsed)
            except ts3.query.TS3TimeoutError:
                break
        
        return f"Added {target} to hunted list"

    def masspoke(self, msg):
        """Poke all connected clients."""
        clients = self.conn.clientlist().parsed
        for client in clients:
            self.conn.clientpoke(msg=msg, clid=client["clid"])
    
    def _fetch_and_log_clientlist(self, conn):
        """Fetch current client list and log to CSV, update client_map."""
        try:
            result = conn.clientlist()
            if not result.parsed:
                logger.warning("Empty client list returned")
                return
            
            clients = []
            for client in result.parsed:
                clid = client.get('clid', '')
                nickname = client.get('client_nickname', '')
                uid = client.get('client_unique_identifier', '')
                ip = client.get('connection_client_ip', '')
                
                # Update client map
                self.client_map[clid] = {
                    'nickname': nickname,
                    'uid': uid,
                    'ip': ip
                }
                
                clients.append({
                    'clid': clid,
                    'nickname': nickname,
                    'uid': uid,
                    'ip': ip
                })
            
            # Log to clients_log.csv
            log_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            clients_log_path = os.path.join(log_dir, 'clients_log.csv')
            ClientListLogger.log_clients(clients_log_path, clients)
            
            logger.info(f"Fetched and logged {len(clients)} clients")
            
        except Exception as e:
            logger.error(f"Failed to fetch/log client list: {e}")
    
    def _fetch_and_update_channels(self, conn):
        """Fetch current channel list and update reference data."""
        try:
            result = conn.channellist()
            if not result.parsed:
                logger.warning("Empty channel list returned")
                return
            
            channels = []
            for channel in result.parsed:
                cid = channel.get('cid', '')
                channel_name = channel.get('channel_name', '')
                
                channels.append({
                    'cid': cid,
                    'channel_name': channel_name
                })
            
            # Update reference manager
            if self.reference_manager:
                self.reference_manager.update_channels(channels)
            
            logger.info(f"Fetched and updated {len(channels)} channels")
            
        except Exception as e:
            logger.error(f"Failed to fetch/update channel list: {e}")
    
    def _reference_data_loop(self):
        """Thread that collects reference data every minute."""
        logger.info("Reference data collection thread started")
        
        while self._running:
            time.sleep(60)  # Wait 1 minute
            
            if not self.conn or not self.conn.is_connected():
                continue
            
            try:
                # Fetch clientlist with all details
                result = self.conn.clientlist(
                    uid=True, 
                    away=True, 
                    voice=True, 
                    times=True, 
                    groups=True, 
                    info=True, 
                    country=True, 
                    ip=True
                )
                
                if result.parsed:
                    clients = []
                    for client in result.parsed:
                        clients.append({
                            'clid': client.get('clid', ''),
                            'client_nickname': client.get('client_nickname', ''),
                            'client_unique_identifier': client.get('client_unique_identifier', ''),
                            'connection_client_ip': client.get('connection_client_ip', '')
                        })
                    
                    # Update reference manager
                    if self.reference_manager:
                        self.reference_manager.update_clients(clients)
                    
                    # Update users seen tracker
                    if self.users_seen_tracker:
                        self.users_seen_tracker.add_users(clients)
                    
                    logger.debug(f"Updated reference data with {len(clients)} clients")
                
                # Fetch channellist
                channel_result = self.conn.channellist()
                if channel_result.parsed:
                    channels = []
                    for channel in channel_result.parsed:
                        channels.append({
                            'cid': channel.get('cid', ''),
                            'channel_name': channel.get('channel_name', '')
                        })
                    
                    # Update reference manager
                    if self.reference_manager:
                        self.reference_manager.update_channels(channels)
                    
                    logger.debug(f"Updated reference data with {len(channels)} channels")
                
                # Check guild exp and notify registered users
                self._check_guild_exp()
                # self._check_friendly_guild_exp()  # Commented out - not needed anymore
                
            except Exception as e:
                error_str = str(e).lower()
                # Check for connection errors
                if any(err in error_str for err in ['broken pipe', 'errno 32', 'connection', 'socket']):
                    logger.warning(f"Reference data collection connection error: {e}")
                    # Mark connection as broken so main loop reconnects it
                    self.conn = None
                    time.sleep(5)
                else:
                    logger.error(f"Error in reference data collection: {e}", exc_info=True)
                    time.sleep(2)
        
        logger.info("Reference data collection thread stopped")
    
    def _check_guild_exp(self):
        """Check guild exp API and notify registered users of gains."""
        try:
            # Configuration
            only_online = True
            guild = "ShellPatrocina"  
            world = "Auroria"
            base_url = "https://rubinot-guild-monitor.onrender.com/"
            
            # Build URL
            url = f"/api/guild-exp?guild={encodeURIComponent(guild)}&world={encodeURIComponent(world)}&only_online={1 if only_online else 0}"
            
            # Make request
            response = requests.get(base_url + url, timeout=10)
            if response.status_code != 200:
                logger.warning(f"Guild exp API returned status {response.status_code}")
                return
            
            data = response.json()
            current_refresh_ts = data.get('last_refresh_ts')
            
            # Skip if no refresh or same as last check
            if not current_refresh_ts:
                logger.debug("No refresh timestamp in guild exp data")
                return
            
            if self.last_guild_refresh_ts == current_refresh_ts:
                logger.debug("Guild exp data hasn't been refreshed since last check")
                return
            
            # Update timestamp
            self.last_guild_refresh_ts = current_refresh_ts
            
            # Get members with exp gains
            members_with_gains = [m for m in data.get('members', []) if m.get('delta_experience', 0) > 0]
            
            if not members_with_gains:
                logger.debug("No guild members with exp gains")
                return
            
            # Format notification message
            refresh_time = datetime.fromtimestamp(current_refresh_ts).strftime('%d/%m/%Y %H:%M:%S')
            message = f"Guild Exp Update ({refresh_time}):\n"
            message += "=" * 50 + "\n\n"
            
            for member in members_with_gains:
                name = member.get('name', 'Unknown')
                delta_exp = member.get('delta_experience', 0)
                level = member.get('level', 0)
                vocation = member.get('vocation', 'Unknown')
                
                # Format exp with thousands separator
                delta_exp_formatted = f"{delta_exp:,}"
                
                message += f"{name} (Lvl {level} {vocation})\n"
                message += f"  +{delta_exp_formatted} exp\n\n"
            
            logger.info(f"Guild exp gains detected for {len(members_with_gains)} members")
            
            # Load registered users
            log_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            registered_file = os.path.join(log_dir, 'registered.txt')
            
            if not os.path.exists(registered_file):
                logger.debug("No registered users for guild exp notifications")
                return
            
            with open(registered_file, 'r', encoding='utf-8') as f:
                registered_uids = set(line.strip() for line in f if line.strip())
            
            if not registered_uids:
                logger.debug("No registered users for guild exp notifications")
                return
            
            # Get current client list to find CLIDs from UIDs
            if not self.conn or not self.conn.is_connected():
                logger.warning("Cannot send guild exp notifications: not connected")
                return
            
            try:
                clients = self.conn.clientlist(uid=True).parsed
                
                # Poke each registered user who is online
                poked_count = 0
                for client in clients:
                    client_uid = client.get('client_unique_identifier', '')
                    if client_uid in registered_uids:
                        clid = client.get('clid')
                        try:
                            self.conn.clientpoke(clid=clid, msg=message)
                            poked_count += 1
                            logger.debug(f"Poked {client.get('client_nickname', 'Unknown')} with guild exp update")
                        except Exception as e:
                            error_str = str(e).lower()
                            if any(err in error_str for err in ['broken pipe', 'errno 32', 'connection', 'socket']):
                                logger.warning(f"Connection error while poking client {clid}: {e}")
                                self.conn = None
                                break  # Stop trying if connection is broken
                            else:
                                logger.error(f"Failed to poke client {clid}: {e}")
                
                if poked_count > 0:
                    logger.info(f"Notified {poked_count} registered users of guild exp gains")
                else:
                    logger.debug("No registered users currently online")
                    
            except Exception as e:
                error_str = str(e).lower()
                if any(err in error_str for err in ['broken pipe', 'errno 32', 'connection', 'socket']):
                    logger.warning(f"Connection error sending guild exp notifications: {e}")
                    self.conn = None
                else:
                    logger.error(f"Error sending guild exp notifications: {e}")
                
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch guild exp data: {e}")
        except Exception as e:
            logger.error(f"Error in guild exp check: {e}")
    
    # COMMENTED OUT - Friendly guild exp checking not needed anymore
    # def _check_friendly_guild_exp(self):
    #     """Check friendly guild exp API and notify registered users of gains."""
    #     try:
    #         # Configuration
    #         only_online = True
    #         guild = "Ascended Auroria"
    #         world = "Auroria"
    #         base_url = "https://rubinot-guild-monitor.onrender.com/"
    #         
    #         # Build URL
    #         url = f"/api/guild-exp?guild={encodeURIComponent(guild)}&world={encodeURIComponent(world)}&only_online={1 if only_online else 0}"
    #         
    #         # Make request
    #         response = requests.get(base_url + url, timeout=10)
    #         if response.status_code != 200:
    #             logger.warning(f"Friendly guild exp API returned status {response.status_code}")
    #             return
    #         
    #         data = response.json()
    #         current_refresh_ts = data.get('last_refresh_ts')
    #         
    #         # Skip if no refresh or same as last check
    #         if not current_refresh_ts:
    #             logger.debug("No refresh timestamp in friendly guild exp data")
    #             return
    #         
    #         if self.last_friendly_guild_refresh_ts == current_refresh_ts:
    #             logger.debug("Friendly guild exp data hasn't been refreshed since last check")
    #             return
    #         
    #         # Update timestamp
    #         self.last_friendly_guild_refresh_ts = current_refresh_ts
    #         
    #         # Get members with exp gains
    #         members_with_gains = [m for m in data.get('members', []) if m.get('delta_experience', 0) > 0]
    #         
    #         if not members_with_gains:
    #             logger.debug("No friendly guild members with exp gains")
    #             return
    #         
    #         # Format notification message
    #         refresh_time = datetime.fromtimestamp(current_refresh_ts).strftime('%d/%m/%Y %H:%M:%S')
    #         message = f"Friendly Guild Exp Update ({refresh_time}):\n"
    #         message += "=" * 50 + "\n\n"
    #         
    #         for member in members_with_gains:
    #             name = member.get('name', 'Unknown')
    #             delta_exp = member.get('delta_experience', 0)
    #             level = member.get('level', 0)
    #             vocation = member.get('vocation', 'Unknown')
    #             
    #             # Format exp with thousands separator
    #             delta_exp_formatted = f"{delta_exp:,}"
    #             
    #             message += f"{name} (Lvl {level} {vocation})\n"
    #             message += f"  +{delta_exp_formatted} exp\n\n"
    #         
    #         logger.info(f"Friendly guild exp gains detected for {len(members_with_gains)} members")
    #         
    #         # Load registered users
    #         log_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    #         registered_file = os.path.join(log_dir, 'registered_friendly.txt')
    #         
    #         if not os.path.exists(registered_file):
    #             logger.debug("No registered users for friendly guild exp notifications")
    #             return
    #         
    #         with open(registered_file, 'r', encoding='utf-8') as f:
    #             registered_uids = set(line.strip() for line in f if line.strip())
    #         
    #         if not registered_uids:
    #             logger.debug("No registered users for friendly guild exp notifications")
    #             return
    #         
    #         # Get current client list to find CLIDs from UIDs
    #         if not self.conn or not self.conn.is_connected():
    #             logger.warning("Cannot send friendly guild exp notifications: not connected")
    #             return
    #         
    #         try:
    #             clients = self.conn.clientlist(uid=True).parsed
    #             
    #             # Poke each registered user who is online
    #             poked_count = 0
    #             for client in clients:
    #                 client_uid = client.get('client_unique_identifier', '')
    #                 if client_uid in registered_uids:
    #                     clid = client.get('clid')
    #                     try:
    #                         self.conn.clientpoke(clid=clid, msg=message)
    #                         poked_count += 1
    #                         logger.debug(f"Poked {client.get('client_nickname', 'Unknown')} with friendly guild exp update")
    #                     except Exception as e:
    #                         error_str = str(e).lower()
    #                         if any(err in error_str for err in ['broken pipe', 'errno 32', 'connection', 'socket']):
    #                             logger.warning(f"Connection error while poking client {clid}: {e}")
    #                             self.conn = None
    #                             break  # Stop trying if connection is broken
    #                         else:
    #                             logger.error(f"Failed to poke client {clid}: {e}")
    #             
    #             if poked_count > 0:
    #                 logger.info(f"Notified {poked_count} registered users of friendly guild exp gains")
    #             else:
    #                 logger.debug("No registered users currently online for friendly guild")
    #                 
    #         except Exception as e:
    #             error_str = str(e).lower()
    #             if any(err in error_str for err in ['broken pipe', 'errno 32', 'connection', 'socket']):
    #                 logger.warning(f"Connection error sending friendly guild exp notifications: {e}")
    #                 self.conn = None
    #             else:
    #                 logger.error(f"Error sending friendly guild exp notifications: {e}")
    #             
    #     except requests.RequestException as e:
    #         logger.warning(f"Failed to fetch friendly guild exp data: {e}")
    #     except Exception as e:
    #         logger.error(f"Error in friendly guild exp check: {e}")
    
    def _update_client_map(self, clid: str, data: dict):
        """Update client map with new data."""
        try:
            if clid not in self.client_map:
                self.client_map[clid] = {'nickname': '', 'uid': '', 'ip': ''}
            
            # Update available fields
            if 'client_nickname' in data:
                self.client_map[clid]['nickname'] = data['client_nickname']
            if 'client_unique_identifier' in data:
                self.client_map[clid]['uid'] = data['client_unique_identifier']
            if 'connection_client_ip' in data:
                self.client_map[clid]['ip'] = data['connection_client_ip']
            
            # Also update reference manager
            if self.reference_manager:
                client_data = [{
                    'clid': clid,
                    'client_nickname': self.client_map[clid]['nickname'],
                    'client_unique_identifier': self.client_map[clid]['uid'],
                    'connection_client_ip': self.client_map[clid]['ip']
                }]
                self.reference_manager.update_clients(client_data)
                
        except Exception as e:
            logger.error(f"Error updating client map: {e}")
    
    def _get_client_info(self, clid: str) -> dict:
        """Get client info from map, return empty dict if not found."""
        return self.client_map.get(clid, {'nickname': '', 'uid': '', 'ip': ''})
    
    def _log_activity(self, event_type: str, clid: str, details: dict):
        """Log activity to CSV."""
        try:
            # Log to old format (backward compatibility)
            if self.activity_logger:
                client_info = self._get_client_info(clid)
                self.activity_logger.log_event(event_type, clid, client_info, details)
            
            # Log to new human-readable format
            if self.human_readable_logger:
                self.human_readable_logger.log_event(clid, event_type, details)
            
        except Exception as e:
            logger.error(f"Error logging activity: {e}")
    
    def _handle_event(self, event_type: str, event_data: dict):
        """Route events to appropriate handlers."""
        try:
            # Extract clid from event data
            clid = event_data.get('clid', '')
            
            # Handle different event types
            if event_type == 'notifycliententerview':
                # Client connected
                self._update_client_map(clid, event_data)
                self._log_activity('cliententerview', clid, event_data)
                logger.debug(f"Client entered: {event_data.get('client_nickname', 'unknown')}")
                
            elif event_type == 'notifyclientleftview':
                # Client disconnected
                self._log_activity('clientleftview', clid, event_data)
                logger.debug(f"Client left: clid={clid}")
                # Don't remove from map - keep for historical reference
                
            elif event_type == 'notifyclientmoved':
                # Client moved channels
                self._log_activity('clientmoved', clid, event_data)
                logger.debug(f"Client moved: clid={clid} from {event_data.get('cfid')} to {event_data.get('ctid')}")
                
            elif event_type == 'notifyclientupdated':
                # Client updated - only log if nickname or mute status changed
                if any(key in event_data for key in ['client_nickname', 'client_input_muted', 'client_output_muted']):
                    old_nickname = self.client_map.get(clid, {}).get('nickname', '')
                    self._update_client_map(clid, event_data)
                    
                    # Add old nickname to details for comparison
                    if 'client_nickname' in event_data:
                        event_data['old_nickname'] = old_nickname
                    
                    self._log_activity('clientupdated', clid, event_data)
                    logger.debug(f"Client updated: clid={clid}")
                
            # Ignore channel edits and other events
            elif event_type in ['notifychanneledited', 'notifychanneldescriptionchanged']:
                pass  # Explicitly ignore
                
        except Exception as e:
            logger.error(f"Error handling event {event_type}: {e}")

    def _event_loop(self):
        """Event handler thread - listens for all events without timeout."""
        logger.info("Event loop thread started")
        
        while self._running:
            if not self.event_conn or not self.event_conn.is_connected():
                time.sleep(1)
                #logger.debug("No event connection, sleeping...")
                continue
            
            try:
                # Wait for events (no timeout - blocking call)
                event = self.event_conn.wait_for_event()

                if event and event.parsed and event._data and len(event._data) > 0:
                    # Extract event type from raw data
                    for i in range(len(event.parsed)):
                        try:
                            event_type = event._data[i].decode("utf-8").split()[0]
                        except (AttributeError, IndexError, UnicodeDecodeError) as e:
                            logger.error(f"Failed to extract event type: {e}, event data: {event._data}")
                            continue
                        
                        event_data = event.parsed[i] if event.parsed else {}
                        
                        # Check for duplicate events within 1 second
                        current_time = time.time()
                        event_signature = (event_type, str(event_data))
                        
                        if (self.last_event == event_signature and 
                            current_time - self.last_event_timestamp < 1.0):
                            logger.debug(f"Ignoring duplicate event: {event_type}")
                            continue
                        
                        # Update last event tracking
                        self.last_event = event_signature
                        self.last_event_timestamp = current_time
                        
                        # Only process commands for text messages
                        if event_type == "notifytextmessage":
                            msg = event_data.get("msg", "")
                            clid = event_data.get("invokerid")
                            nickname = event_data.get("invokername", "")
                            
                            # Ignore messages from x3tBot and from the bot itself
                            if "x3tBot" in nickname or "x3t" in nickname.lower() or nickname == self.nickname:
                                logger.debug("Ignoring message from %s", nickname)
                            else:
                                # Enqueue command for worker thread to process
                                try:
                                    self.command_queue.put((msg, clid, nickname))
                                    logger.debug(f"Enqueued command from {nickname}: {msg[:20]}...")
                                except Exception as e:
                                    logger.error("Error enqueueing command: %s", e)
                        else:
                            # Route all other events to activity logger
                            try:
                                self._handle_event(event_type, event_data)
                            except Exception as e:
                                logger.error(f"Error handling event {event_type}: {e}")
            
            except ts3.query.TS3TimeoutError:
                # Should not happen without timeout, but handle anyway
                pass
            
            except Exception as e:
                if self._running:  # Only log if we're supposed to be running
                    error_str = str(e).lower()
                    # Check for connection errors
                    if any(err in error_str for err in ['broken pipe', 'errno 32', 'connection', 'socket']):
                        logger.warning(f"Event connection error: {e}")
                        # Mark connection as broken so it gets reconnected
                        self.event_conn = None
                        time.sleep(2)
                    else:
                        logger.error(f"Error in event loop: {e}", exc_info=True)
                        time.sleep(1)
        
        logger.info("Event loop thread stopped")

    def _worker_loop(self):
        """Worker thread - processes commands from queue."""
        logger.info("Worker loop thread started")
        
        while self._running:
            try:
                # Get command from queue (blocking with timeout)
                try:
                    msg, clid, nickname = self.command_queue.get(timeout=1)
                except:
                    # Timeout - no command in queue
                    continue
                
                # Check if worker connection is available
                if not self.worker_conn or not self.worker_conn.is_connected():
                    logger.warning("Worker connection not available, dropping command")
                    self.command_queue.task_done()
                    continue
                
                # Process command and send response
                try:
                    logger.debug(f"Processing command from {nickname}: {msg[:20]}...")
                    response = process_command(self, msg, nickname)
                    
                    # Try to send response with retry logic
                    max_retries = 2
                    for attempt in range(max_retries):
                        try:
                            self.worker_conn.sendtextmessage(targetmode=1, target=clid, msg=response)
                            logger.debug(f"Sent response to {nickname}")
                            break  # Success, exit retry loop
                        except Exception as send_error:
                            error_str = str(send_error).lower()
                            # Check for connection errors
                            if any(err in error_str for err in ['broken pipe', 'errno 32', 'connection', 'socket']):
                                logger.warning(f"Connection error on attempt {attempt + 1}/{max_retries}: {send_error}")
                                # Mark connection as broken
                                self.worker_conn = None
                                
                                if attempt < max_retries - 1:
                                    # Wait a bit before retry
                                    time.sleep(0.5)
                                    # Connection will be re-established by main loop
                                    if not self.worker_conn:
                                        logger.debug("Waiting for worker connection to be re-established...")
                                        time.sleep(1)
                                else:
                                    logger.error(f"Failed to send response after {max_retries} attempts: {send_error}")
                            else:
                                # Non-connection error, don't retry
                                logger.error(f"Error sending message: {send_error}")
                                break
                                
                except Exception as e:
                    logger.error(f"Error processing command in worker: {e}", exc_info=True)
                finally:
                    self.command_queue.task_done()
                    
            except Exception as e:
                if self._running:
                    logger.error(f"Error in worker loop: {e}", exc_info=True)
                time.sleep(1)
        
        logger.info("Worker loop thread stopped")

    def run(self):
        """Main event loop."""
        logger.info("Starting bot...")
        self._running = True
        
        # Create main connection for operations
        try:
            self.conn = self.setup_connection()
            logger.info("Main connection established")
        except Exception as e:
            logger.error("Failed to establish main connection: %s", e)
            self._reconnect(e)
        
        # Create dedicated connection for event loop
        try:
            self.event_conn = self.setup_event_connection()
            logger.info("Event connection established")
        except Exception as e:
            logger.error("Failed to establish event connection: %s", e)
        
        # Create dedicated connection for worker thread
        try:
            self.worker_conn = self.setup_worker_connection()
            logger.info("Worker connection established")
        except Exception as e:
            logger.error("Failed to establish worker connection: %s", e)
        
        logger.info("Setting up event loop thread...")
        self._event_thread = threading.Thread(target=self._event_loop, daemon=True)
        self._event_thread.start()
        logger.info("Event thread started")
        
        logger.info("Setting up worker thread...")
        self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self._worker_thread.start()
        logger.info("Worker thread started")
        
        # Start reference data collection thread
        logger.info("Setting up reference data collection thread...")
        self._reference_thread = threading.Thread(target=self._reference_data_loop, daemon=True)
        self._reference_thread.start()
        logger.info("Reference data collection thread started")

        try:
            while self._running:
                # Reconnect main connection if needed
                if self.conn is None or not self.conn.is_connected():
                    try:
                        logger.info("Main connection not available, attempting to reconnect...")
                        self.conn = self.setup_connection()
                        logger.info("Main connection re-established")
                    except Exception as e:
                        logger.error(f"Failed to reconnect main connection: {e}")
                        time.sleep(5)
                        continue
                
                # Reconnect event connection if needed
                if self.event_conn is None or not self.event_conn.is_connected():
                    try:
                        logger.info("Event connection not available, attempting to reconnect...")
                        self.event_conn = self.setup_event_connection()
                        logger.info("Event connection re-established")
                        
                        # Restart event thread if not running
                        if not self._event_thread or not self._event_thread.is_alive():
                            logger.info("Restarting event loop thread...")
                            self._event_thread = threading.Thread(target=self._event_loop, daemon=True)
                            self._event_thread.start()
                    except Exception as e:
                        logger.error("Failed to reconnect event connection: %s", e)
                        time.sleep(2)
                        continue
                
                # Reconnect worker connection if needed
                if self.worker_conn is None or not self.worker_conn.is_connected():
                    try:
                        logger.info("Worker connection not available, attempting to reconnect...")
                        self.worker_conn = self.setup_worker_connection()
                        logger.info("Worker connection re-established")
                        
                        # Restart worker thread if not running
                        if not self._worker_thread or not self._worker_thread.is_alive():
                            logger.info("Restarting worker thread...")
                            self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
                            self._worker_thread.start()
                    except Exception as e:
                        logger.error("Failed to reconnect worker connection: %s", e)
                        time.sleep(2)
                        continue
                
                # Send keepalive and check connection health
                try:
                    self.conn.send_keepalive()
                    self.event_conn.send_keepalive()
                    self.worker_conn.send_keepalive()
                    # Ensure all connections are still connected to the server
                    self._ensure_server_connection(self.conn, "Main connection")
                    self._ensure_server_connection(self.event_conn, "Event connection")
                    self._ensure_server_connection(self.worker_conn, "Worker connection")
                except Exception as e:
                    error_str = str(e).lower()
                    if any(err in error_str for err in ['broken pipe', 'errno 32', 'connection', 'socket']):
                        logger.warning(f"Keepalive connection error: {e}")
                        # Mark connections as broken
                        self.conn = None
                        self.event_conn = None
                    else:
                        logger.error(f"Keepalive failed: {e}")
                        self.conn = None
                
                time.sleep(3)
        
        except KeyboardInterrupt:
            logger.info("Bot shutdown requested")
            self._running = False
        except Exception as e:
            logger.critical(f"Critical error in main loop: {e}", exc_info=True)
            logger.info("Bot will attempt to continue...")
            time.sleep(5)
        
        finally:
            # Cleanup
            self._running = False
            if self._event_thread:
                self._event_thread.join(timeout=5)
            if self._worker_thread:
                self._worker_thread.join(timeout=5)
            if self._reference_thread:
                self._reference_thread.join(timeout=5)
            if self.event_conn:
                try:
                    self.event_conn.close()
                except Exception as e:
                    logger.error(f"Error closing event connection: {e}")
            if self.worker_conn:
                try:
                    self.worker_conn.close()
                except Exception as e:
                    logger.error(f"Error closing worker connection: {e}")
            if self.conn:
                try:
                    self.conn.close()
                except Exception as e:
                    logger.error(f"Error closing main connection: {e}")
            if self.activity_logger:
                try:
                    self.activity_logger.close()
                except Exception as e:
                    logger.error(f"Error closing activity logger: {e}")
            if self.human_readable_logger:
                try:
                    self.human_readable_logger.close()
                except Exception as e:
                    logger.error(f"Error closing human-readable logger: {e}")
            logger.info("Bot stopped")
            