import logging
from logging.handlers import TimedRotatingFileHandler
import os
from datetime import datetime

def setup_logger(name, log_file, level=logging.INFO):
    """Setup a logger with file and console handlers"""
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # Create file handler
    file_handler = TimedRotatingFileHandler(
        filename=log_file,
        when="midnight",
        interval=1,
        backupCount=7,
        encoding='utf-8',
        utc=False
    )
    file_handler.setFormatter(formatter)
    file_handler.suffix = "%Y-%m-%d"
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Avoid adding handlers multiple times in case of module reload
    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    
    return logger

# Ensure log directory exists
log_folder = 'log_app'
if not os.path.exists(log_folder):
    os.makedirs(log_folder)

# Get current date for log filenames
date_str = datetime.now().strftime('%Y-%m-%d')

# Main application logger
main_log_file = os.path.join(log_folder, f'cnote_sync_{date_str}.log')
logger = setup_logger('CnoteLogger', main_log_file)

# CTC Sync specific logger
ctc_log_file = os.path.join(log_folder, f'sync_ctc_tci_tco_{date_str}.log')
ctc_logger = setup_logger('CTCSyncLogger', ctc_log_file)
