#!/usr/bin/env python3
"""
TSBOTRPI - TeamSpeak Bot for Raspberry Pi
Entry point for the bot application.
"""
import logging
import os
import sys

# Add src to path
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
SRC_PATH = os.path.join(PROJECT_ROOT, "src")
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

from tsbotrpi.bot import TS3Bot
from tsbotrpi.config import load_config
from tsbotrpi.tsclient import TSClientManager


def main():
    """Main entry point."""
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger = logging.getLogger(__name__)
    
    try:
        # Load config
        config = load_config()
        logger.info("Bot starting...")
        logger.info("Host: %s", config["host"])
        
        # Setup TS client manager (Raspberry Pi)
        client_manager = None
        if config["client_command"]:
            client_manager = TSClientManager(
                command=config["client_command"],
                pid_file=config["pid_file"]
            )
            client_manager.start()
        
        # Create and run bot
        bot = TS3Bot(
            host=config["host"],
            api_key=config["api_key"],
            process_manager=client_manager
        )
        bot.run()
    
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error("Fatal error: %s", e, exc_info=True)
        sys.exit(1)
    finally:
        # Cleanup
        if client_manager:
            client_manager.stop()


if __name__ == "__main__":
    main()
