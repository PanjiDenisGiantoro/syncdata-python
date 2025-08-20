import schedule
import time

from .flight_log import insertFlightLog
from .flight_fetch_airlabs import insertFlightBigIata

def run_schedule_flight():
    schedule.every().day.at("07:00").do(insertFlightLog) #17:00 PST = 08:00 WIB
    schedule.every().day.at("16:00").do(insertFlightBigIata)
    while True:
        schedule.run_pending()
        time.sleep(1)