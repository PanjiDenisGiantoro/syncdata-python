import schedule
import time

from . import run_backup
from .backup_utils import DatabaseBackup

def run_schedule_sync_ctc():
    schedule.every().day.at("17:37").do(run_backup) #17:00 PST = 08:00 WIB
    while True:
        schedule.run_pending()
        time.sleep(1)