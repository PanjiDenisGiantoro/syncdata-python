from datetime import datetime, timedelta
from db import get_oracle_connection_dbrbn, get_oracle_connection_billing

def insert_monitoring_data(module, total_reborn, total_billing, periode,
                          createdby='SYSTEM', lastupdby='SYSTEM', lastupdprocess='monitoring', deleted=0):
    connection = get_oracle_connection_billing()  # Atur ke DB yang menyimpan tabel monitoring
    if not connection:
        print('Gagal koneksi ke database monitoring!')
        return False
    cursor = connection.cursor()
    now = datetime.now()
    sql = '''
        INSERT INTO MONITORING_DATA (
            MODULE, TOTAL_REBORN, TOTAL_BILLING, CREATE_DATE, UPDATE_AT, PERIODE,
            CREATEDTM, CREATEDBY, LASTUPDTM, LASTUPDBY, LASTUPDPROCESS, DELETED
        ) VALUES (
            :module, :total_reborn, :total_billing, :create_date, :update_at, :periode,
            :createdtm, :createdby, :lastupdtm, :lastupdby, :lastupdprocess, :deleted
        )
    '''
    params = {
        'module': module,
        'total_reborn': total_reborn,
        'total_billing': total_billing,
        'create_date': now,
        'update_at': now,
        'periode': periode,
        'createdtm': now,
        'createdby': createdby,
        'lastupdtm': now,
        'lastupdby': lastupdby,
        'lastupdprocess': lastupdprocess,
        'deleted': deleted
    }
    cursor.execute(sql, params)
    connection.commit()
    cursor.close()
    connection.close()
    return True

def monitoring_cnote_count(month=None, year=None):
    # Default: bulan & tahun sekarang
    now = datetime.now()
    if not month:
        month = now.month
    if not year:
        year = now.year
    start_date = datetime(year, month, 1)
    if month == 12:
        end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        end_date = datetime(year, month + 1, 1) - timedelta(days=1)
    query = """
        SELECT COUNT(*) FROM CMS_CNOTE
        WHERE CNOTE_CRDATE >= :start_date AND CNOTE_CRDATE <= :end_date
    """
    def get_count(conn_func):
        conn = conn_func()
        if not conn:
            return None
        cur = conn.cursor()
        cur.execute(query, {'start_date': start_date, 'end_date': end_date})
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return count
    count_dbrbn = get_count(get_oracle_connection_dbrbn)
    count_billing = get_count(get_oracle_connection_billing)
    periode = f"{year}-{month:02d}-01"
    insert_monitoring_data(
        module='CN',
        total_reborn=count_dbrbn,
        total_billing=count_billing,
        periode=periode
    )
    return {
        "periode": periode,
        "total_reborn": count_dbrbn,
        "total_billing": count_billing,
        "status": "MATCH" if count_dbrbn == count_billing else "NOT MATCH"
    }