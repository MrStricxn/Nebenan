import logging
import os
from logging.handlers import RotatingFileHandler

import uvicorn
from src.webapi import app


def setup_file_logging() -> None:
    """Persist 'nebena' logs to logs/nebena.log (rotating 5MB x3).

    Console + dashboard WS logging are wired elsewhere; without this,
    `python web.py` keeps logs only in the terminal.
    """
    os.makedirs("logs", exist_ok=True)
    logger = logging.getLogger("nebena")
    logger.setLevel(logging.DEBUG)
    if any(isinstance(h, RotatingFileHandler) for h in logger.handlers):
        return
    fh = RotatingFileHandler(
        "logs/nebena.log", maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(fh)


if __name__ == "__main__":
    setup_file_logging()
    print("Nebenan.de Web UI → http://localhost:8000")
    print("Логи: logs/nebena.log")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")
