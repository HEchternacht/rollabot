import logging
import time
import ts3

logger = logging.getLogger(__name__)


class TS3Bot:
    """Simplified TeamSpeak bot based on AutoanswerScheduler pattern."""

    def __init__(self, host: str, api_key: str, process_manager=None):
        self.host = host
        self.api_key = api_key
        self.process_manager = process_manager
        self.conn = None

    def setup_connection(self):
        """Setup TS3 ClientQuery connection."""
        conn = ts3.query.TS3ClientConnection(self.host)
        conn.auth(apikey=self.api_key)
        conn.clientnotifyregister(event="notifytextmessage", schandlerid=1)
        logger.info("Connected to %s", self.host)
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
                self.process_manager.restart()
                time.sleep(5)  # Wait for TS client to start
                
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

    def answer(self, msg, nickname):
        """Process commands and return response."""
        if msg.startswith("!mp"):
            self.masspoke(f"{nickname} te cutucou: {msg[4:]}")
            return "Poking all clients..."
        
        if msg.startswith("!hunted add"):
            target = msg[12:].strip()
            return self.add_hunted(target)
        
        if msg.startswith("!snapshot"):
            snapshot = self.conn.clientlist(
                info=True, country=True, uid=True, ip=True,
                groups=True, times=True, voice=True, away=True
            ).parsed
            return str(snapshot)
        
        return f"Unknown command: {msg}"

    def run(self):
        """Main event loop."""
        logger.info("Starting bot...")
        
        while True:
            # Ensure connection
            if not self.conn:
                self._reconnect()
                continue
            
            try:
                # Wait for events
                event = self.conn.wait_for_event(timeout=3)
                if event.parsed:
                    msg = event.parsed[0].get("msg", "")
                    clid = event.parsed[0].get("invokerid")
                    nickname = event.parsed[0].get("invokername", "")
                    
                    # Process and respond
                    response = self.answer(msg, nickname)
                    self.conn.sendtextmessage(targetmode=1, target=clid, msg=response)
            
            except ts3.query.TS3TimeoutError:
                # Normal timeout - send keepalive
                try:
                    self.conn.send_keepalive()
                except:
                    self._reconnect()
            
            except Exception as e:
                logger.error("Error in event loop: %s", e)
                self._reconnect(e)