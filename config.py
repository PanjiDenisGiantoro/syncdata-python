# class Config:
#     # Koneksi ke database ctcv2
#     DB_USER_CTCV2 = 'Dbctc_v2'
#     DB_PASSWORD_CTCV2 = 'dbctc123'
#     DB_DSN_CTCV2 = '10.8.2.48:1521/ctcv2db'
#
#     # Koneksi ke database Billing
#     DB_USER_BILLING = 'JNEBILL'
#     DB_PASSWORD_BILLING = 'JNE98292092B5494083OK'
#     DB_DSN_BILLING = '10.8.2.219:1521/JNEBILL'
#
#     # Koneksi ke database dbrbn
#     DB_USER_DBRBN = 'APPSJNE'
#     DB_PASSWORD_DBRBN = 'JNE98292092B5494083OK'
#     DB_DSN_DBRBN = '10.8.2.219:1521/ORIONRBN'
#
#     # Koneksi ke database training
#     DB_USER_TRAINING = 'JNEBILL'
#     DB_PASSWORD_TRAINING = 'JNEBILL'
#     DB_DSN_TRAINING = '10.8.2.19:1522/TRAINING'



# Memuat variabel dari file .env

import os
from dotenv import load_dotenv
load_dotenv()

class Config:
    # Membaca kredensial database dari .env
    DB_USER_CTCV2 = os.getenv('DB_USER_CTCV2')
    DB_PASSWORD_CTCV2 = os.getenv('DB_PASSWORD_CTCV2')
    DB_DSN_CTCV2 = os.getenv('DB_DSN_CTCV2')

    DB_USER_BILLING = os.getenv('DB_USER_BILLING')
    DB_PASSWORD_BILLING = os.getenv('DB_PASSWORD_BILLING')
    DB_DSN_BILLING = os.getenv('DB_DSN_BILLING')

    DB_USER_DBRBN = os.getenv('DB_USER_DBRBN')
    DB_PASSWORD_DBRBN = os.getenv('DB_PASSWORD_DBRBN')
    DB_DSN_DBRBN = os.getenv('DB_DSN_DBRBN')

    DB_USER_TRAINING = os.getenv('DB_USER_TRAINING')
    DB_PASSWORD_TRAINING = os.getenv('DB_PASSWORD_TRAINING')
    DB_DSN_TRAINING = os.getenv('DB_DSN_TRAINING')

    DB_USER_JNE = os.getenv('DB_USER_JNE')
    DB_PASSWORD_JNE = os.getenv('DB_PASSWORD_JNE')
    DB_DSN_JNE = os.getenv('DB_DSN_JNE')
