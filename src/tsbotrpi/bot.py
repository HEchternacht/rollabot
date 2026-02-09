import logging
import time
import threading
import os
import ts3

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
        self.conn = None
        self._event_thread = None
        self._reference_thread = None
        self._running = False
        self.client_map = {}  # Maps clid -> {nickname, uid, ip}
        self.activity_logger = None
        self.clients_logger_initialized = False
        
        # New components
        self.reference_manager = None
        self.users_seen_tracker = None
        self.human_readable_logger = None

    def setup_connection(self):
        """Setup TS3 ClientQuery connection."""
        conn = ts3.query.TS3ClientConnection(self.host)
        conn.auth(apikey=self.api_key)
        conn.use()
        # Register for ALL notifications, not just text messages
        conn.clientnotifyregister(event="any", schandlerid=1)
        
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
            self.clients_logger_initialized = True
        
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
        
        # Try to connect first
        try:
            self.conn = self.setup_connection()
            logger.info("Reconnected successfully")
            return
        except Exception as e:
            logger.error("Connection failed: %s", e)
            
            # Only start/restart TS client if connection refused or address not found
            if self._is_connection_refused(e) and self.process_manager:
                logger.warning("Connection refused/unavailable - starting TS client")
                restarted = self.process_manager.restart()
                
                # Wait longer if we actually restarted (box64 takes ~60s)
                # Wait less if restart was skipped due to cooldown
                wait_time = 60 if restarted else 5
                logger.info("Waiting %ds for TS client...", wait_time)
                time.sleep(wait_time)
                
                # Try connecting again
                try:
                    self.conn = self.setup_connection()
                    logger.info("Connected after starting TS client")
                except Exception as e2:
                    logger.error("Still cannot connect: %s", e2)
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
                
            except Exception as e:
                logger.error(f"Error in reference data collection: {e}")
        
        logger.info("Reference data collection thread stopped")
    
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
            if not self.conn or not self.conn.is_connected():
                time.sleep(1)
                logger.debug("No connection in event loop, sleeping...")
                continue
            
            try:
                # Wait for events (no timeout - blocking call)
                event = self.conn.wait_for_event()
                
                if event.parsed and event._data and len(event._data) > 0:
                    # Extract event type from raw data
                    try:
                        event_type = event._data[0].decode("utf-8").split()[0]
                    except (AttributeError, IndexError, UnicodeDecodeError) as e:
                        logger.error(f"Failed to extract event type: {e}")
                        continue
                    
                    event_data = event.parsed[0] if event.parsed else {}
                    
                    # Only process commands for text messages
                    if event_type == "notifytextmessage":
                        msg = event_data.get("msg", "")
                        clid = event_data.get("invokerid")
                        nickname = event_data.get("invokername", "")
                        
                        # Ignore messages from x3tBot
                        if "x3tBot" in nickname or "x3t" in nickname.lower():
                            logger.debug("Ignoring message from %s", nickname)
                        else:
                            # Process and respond
                            try:
                                response = process_command(self, msg, nickname)
                                self.conn.sendtextmessage(targetmode=1, target=clid, msg=response)
                            except Exception as e:
                                logger.error("Error processing command: %s", e)
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
                    logger.error("Error in event loop: %s", e)
                time.sleep(1)
        
        logger.info("Event loop thread stopped")

    def run(self):
        """Main event loop."""
        logger.info("Starting bot...")
        self._running = True
        logger.info("Setting up event loop thread...")
        self._event_thread = threading.Thread(target=self._event_loop, daemon=True)
        self._event_thread.start()
        logger.info("Event thread started")
        
        # Start reference data collection thread
        logger.info("Setting up reference data collection thread...")
        self._reference_thread = threading.Thread(target=self._reference_data_loop, daemon=True)
        self._reference_thread.start()
        logger.info("Reference data collection thread started")

        try:
            while self._running:
               
                try:
                    self.conn = self.setup_connection()
                    
                    # Start event thread if not running
                    if not self._event_thread or not self._event_thread.is_alive():
                        logger.info("re:Starting event loop thread...")
                        self._event_thread = threading.Thread(target=self._event_loop, daemon=True)
                        self._event_thread.start()
                        logger.info("Event thread started")
                except Exception as e:
                    #logger.error("Connection failed: %s", e)
                    self._reconnect(e)
                    time.sleep(2)
                    continue
                
                # Send keepalive and check connection health
                try:
        
                    self.conn.send_keepalive()
                except Exception as e:
                    logger.error("Keepalive failed: %s", e)
                    self.conn = None
                
                time.sleep(3)
        
        finally:
            # Cleanup
            self._running = False
            if self._event_thread:
                self._event_thread.join(timeout=5)
            if self._reference_thread:
                self._reference_thread.join(timeout=5)
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
            