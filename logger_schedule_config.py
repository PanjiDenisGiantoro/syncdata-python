import logging
import os

from datetime import datetime
from logging.handlers import TimedRotatingFileHandler

log_schedule_folder = 'log_schedule'
if not os.path.exists(log_schedule_folder):
    os.makedirs(log_schedule_folder)

date_str = datetime.now().strftime('%Y-%m-%d')
log_schedule_filename = os.path.join(log_schedule_folder, f'flight_schedule{date_str}.log')

handler = TimedRotatingFileHandler(
    filename=log_schedule_filename,
    when="midnight",
    interval=1,
    backupCount=7,
    encoding='utf-8',
    utc=False
)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger = logging.getLogger("flgihtLogger")
logger.setLevel(logging.INFO)

if not logger.handlers:
    logger.addHandler(handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

