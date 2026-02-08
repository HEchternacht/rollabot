import logging
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
SRC_PATH = os.path.join(PROJECT_ROOT, "src")
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

from tsbotrpi.bot import TS3Bot
from tsbotrpi.config import load_settings
from tsbotrpi.tsclient import TSClientProcessManager


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    settings = load_settings()
    process_manager = TSClientProcessManager(
        command=settings.client_command,
        workdir=settings.client_workdir,
        pid_file=settings.client_pid_file,
    )

    if settings.client_command:
        process_manager.start()

    bot = TS3Bot(settings=settings, process_manager=process_manager)
    try:
        bot.run()
    except KeyboardInterrupt:
        pass
    finally:
        if settings.client_command:
            process_manager.stop()


if __name__ == "__main__":
    main()
