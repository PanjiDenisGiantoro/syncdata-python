import schedule
import time

from .insert_tco_tci_v2 import sync_run_ctc

def run_schedule_sync_ctc():
    schedule.every().day.at("13:18").do(sync_run_ctc) #17:00 PST = 08:00 WIB
    while True:
        schedule.run_pending()
        time.sleep(1)