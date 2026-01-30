import logging
import os
from datetime import datetime

LOGS_DIR = "Logs"
os.makedirs(LOGS_DIR, exist_ok=True)

LOG_FILE_NAME = os.path.join(
    LOGS_DIR, f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
)

logging.basicConfig(
    filename=LOG_FILE_NAME,
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    return logger