import logging
import time
import threading
import ts3

from .commands import process_command

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
        self._running = False

    def setup_connection(self):
        """Setup TS3 ClientQuery connection."""
        conn = ts3.query.TS3ClientConnection(self.host)
        conn.auth(apikey=self.api_key)
        conn.use()
        conn.clientnotifyregister(event="notifytextmessage", schandlerid=1)
        
        # Connect to server if address is configured
        if self.server_address:
            try:
                conn.send(f"connect address={self.server_address} nickname={self.nickname}")
                logger.info("Connected to server %s as %s", self.server_address, self.nickname)
            except Exception as e:
                # Ignore "already connected" error (id 1796)
                if "1796" in str(e) or "currently not possible" in str(e).lower():
                    logger.debug("Already connected to server")
                else:
                    logger.warning("Connect command failed: %s", e)
        
        logger.info("Connected to ClientQuery at %s", self.host)
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

    def _event_loop(self):
        """Event handler thread - listens for messages without timeout."""
        logger.info("Event loop thread started")
        
        while self._running:
            if not self.conn:
                time.sleep(0.5)
                continue
            
            try:
                # Wait for events (no timeout - blocking call)
                event = self.conn.wait_for_event()
                
                if event.parsed:
                    msg = event.parsed[0].get("msg", "")
                    clid = event.parsed[0].get("invokerid")
                    nickname = event.parsed[0].get("invokername", "")
                    
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
        
        try:
            while self._running:
                # Ensure connection
                if not self.conn or not self.conn.is_connected():
                    try:
                        self.conn = self.setup_connection()
                        
                        # Start event thread if not running
                        if not self._event_thread or not self._event_thread.is_alive():
                            self._event_thread = threading.Thread(target=self._event_loop, daemon=True)
                            self._event_thread.start()
                    
                    except Exception as e:
                        logger.error("Connection failed: %s", e)
                        self._reconnect(e)
                        time.sleep(2)
                        continue
                
                # Send keepalive and check connection health
                try:
                    if self.conn and self.conn.is_connected():
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
            logger.info("Bot stopped")
            