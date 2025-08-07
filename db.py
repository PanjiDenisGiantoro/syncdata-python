import oracledb
from config import Config

# Koneksi ke database Oracle - ctcv2db
def get_oracle_connection_ctcv2db():
    try:
        connection = oracledb.connect(
            user=Config.DB_USER_CTCV2,
            password=Config.DB_PASSWORD_CTCV2,
            dsn=Config.DB_DSN_CTCV2
        )
        return connection
    except oracledb.DatabaseError as e:
        print(f"Error while connecting to Oracle ctcv2db: {e}")
        return None


# Koneksi ke database Oracle - Billing
def get_oracle_connection_billing():
    try:
        connection = oracledb.connect(
            user=Config.DB_USER_BILLING,
            password=Config.DB_PASSWORD_BILLING,
            dsn=Config.DB_DSN_BILLING
        )
        return connection
    except oracledb.DatabaseError as e:
        print(f"Error while connecting to Oracle Billing: {e}")
        return None


# Koneksi ke database Oracle - dbrbn
def get_oracle_connection_dbrbn():
    try:
        connection = oracledb.connect(
            user=Config.DB_USER_DBRBN,
            password=Config.DB_PASSWORD_DBRBN,
            dsn=Config.DB_DSN_DBRBN
        )
        return connection
    except oracledb.DatabaseError as e:
        print(f"Error while connecting to Oracle dbrbn: {e}")
        return None

def get_oracle_connection_training():
    try:
        connection = oracledb.connect(
            user=Config.DB_USER_TRAINING,
            password=Config.DB_PASSWORD_TRAINING,
            dsn=Config.DB_DSN_TRAINING
        )
        return connection
    except oracledb.DatabaseError as e:
        print(f"Error while connecting to Oracle training: {e}")
        return None