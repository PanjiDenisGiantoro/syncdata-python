# utils/time_utils.py
from datetime import datetime, timedelta
import pytz

def get_jakarta_time():
    jakarta_tz = pytz.timezone('Asia/Jakarta')
    return datetime.now(jakarta_tz)
