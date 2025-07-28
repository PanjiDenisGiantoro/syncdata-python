import logging
from logging.handlers import TimedRotatingFileHandler
import os
from datetime import datetime

# Buat folder log jika belum ada
log_folder = 'log_app'
if not os.path.exists(log_folder):
    os.makedirs(log_folder)

# Buat nama file log berdasarkan tanggal hari ini
date_str = datetime.now().strftime('%Y-%m-%d')
log_filename = os.path.join(log_folder, f'cnote_sync_{date_str}.log')

# Buat handler dengan rotasi setiap hari
# NOTE: Nama file akan tetap sama selama aplikasi berjalan hari itu
# Tapi file lama akan di-rotate dan disimpan otomatis oleh TimedRotatingFileHandler
handler = TimedRotatingFileHandler(
    filename=log_filename,
    when="midnight",
    interval=1,
    backupCount=7,
    encoding='utf-8',
    utc=False
)

# Format log
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Buat logger utama yang bisa di-import dari mana saja
logger = logging.getLogger("CnoteLogger")
logger.setLevel(logging.INFO)

# Hindari penambahan handler berkali-kali saat module di-import ulang
if not logger.handlers:
    logger.addHandler(handler)

    # Tambahkan juga handler untuk console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
