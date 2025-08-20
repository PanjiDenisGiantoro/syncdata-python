from datetime import datetime
from db import get_oracle_connection_dbrbn, get_oracle_connection_billing
import json
import os

from logger_config import log_filename


def extract_error_from_log(log_date):
    log_folder = 'log_app'
    log_filename = os.path.join(log_folder,f'cnote_sync_{log_date}.log')

    if not os.path.exists(log_filename):
        return []

    error_logs = []
    with open(log_filename, 'r', encoding='utf-8') as file:
        for line in file:
            if 'ERROR' in line:
                # Contoh format: 2025-07-28 11:03:47,875 - ERROR - [p_sync_cnote_upd_process] Job ID xxxxx: Failed to ...
                try:
                    parts = line.strip().split(" - ")
                    if len(parts) >= 3:
                        timestamp = parts[0]
                        message = parts[2]
                        error_logs.append({
                            "timestamp": timestamp,
                            "message": message
                        })
                except Exception:
                    continue
    return error_logs

def p_monitoring_sync_cnote(module, total_reborn, total_billing, periode,
                            total_bill_flag=0, remark=None, total_cnote_update=0,
                            createdby='SYSTEM', lastupdby='SYSTEM',
                            lastupdprocess='monitoring', deleted=0):
    connection = get_oracle_connection_billing()
    if not connection:
        print('Gagal koneksi ke database monitoring!')
        return False

    cursor = connection.cursor()
    now = datetime.now()

    query_check_date = '''
    SELECT COUNT(*) 
    FROM monitoring_sync_cnote 
    WHERE PERIODE = :periode
    '''
    cursor.execute(query_check_date, {'periode': periode})
    count = cursor.fetchone()[0]

    if count > 0:
        sql = '''
              UPDATE monitoring_sync_cnote 
              SET TOTAL_REBORN = :total_reborn,
                  TOTAL_BILLING = :total_billing,
                  TOTAL_BILL_FLAG = :total_bill_flag,
                  TOTAL_CNOTE_UPDATE = :total_cnote_update,
                  REMARK = :remark,
                  UPDATE_AT = :update_at,
                  LASTUPDTM = :lastupdtm,
                  LASTUPDBY = :lastupdby,
                  LASTUPDPROCESS = :lastupdprocess,
                  DELETED = :deleted 
              WHERE PERIODE = :periode
              '''
        params = {
            'total_reborn': total_reborn,
            'total_billing': total_billing,
            'total_bill_flag': total_bill_flag,
            'total_cnote_update': total_cnote_update,
            'remark': remark,
            'update_at': now,
            'lastupdtm': now,
            'lastupdby': lastupdby,
            'lastupdprocess': lastupdprocess,
            'deleted': deleted,
            'periode': periode
        }
    else:
        sql = '''
          INSERT INTO monitoring_sync_cnote (
              ID, MODULE, TOTAL_REBORN, TOTAL_BILLING, TOTAL_BILL_FLAG, REMARK,
              CREATE_DATE, UPDATE_AT, PERIODE, CREATEDTM, CREATEDBY,
              LASTUPDTM, LASTUPDBY, LASTUPDPROCESS, DELETED, TOTAL_CNOTE_UPDATE
          ) VALUES (
              seq_monitoring_sync_cnote_id.NEXTVAL, :module, :total_reborn, :total_billing,
              :total_bill_flag, :remark, :create_date, :update_at, :periode,
              :createdtm, :createdby, :lastupdtm, :lastupdby, :lastupdprocess, :deleted, :total_cnote_update
          )
          '''
        params = {
            'module': module,
            'total_reborn': total_reborn,
            'total_billing': total_billing,
            'total_bill_flag': total_bill_flag,
            'remark': remark,
            'create_date': now,
            'update_at': now,
            'periode': periode,
            'createdtm': now,
            'createdby': createdby,
            'lastupdtm': now,
            'lastupdby': lastupdby,
            'lastupdprocess': lastupdprocess,
            'deleted': deleted,
            'total_cnote_update': total_cnote_update
        }

    cursor.execute(sql, params)
    connection.commit()
    cursor.close()
    connection.close()
    return True


def monitoring_cnote_count_today():
    today_date = datetime.now().strftime('%Y-%m-%d')

    query_total = """
        SELECT COUNT(*) 
        FROM CMS_CNOTE
        WHERE TRUNC(CNOTE_CRDATE) = TRUNC(SYSDATE) - 1
    """

    query_unbilled = """
        SELECT COUNT(A.CNOTE_NO)
        FROM CMS_CNOTE B,
             REPJNE.CONNOTE_UPDATE A
        WHERE BILL_FLAG = 'N'
          AND TRUNC(CDATE) = TRUNC(SYSDATE) - 1
          AND A.CNOTE_NO = B.CNOTE_NO(+)
    """

    query_connote_update = """
        SELECT COUNT(A.CNOTE_NO)
        FROM CMS_CNOTE B,
             REPJNE.CONNOTE_UPDATE A
        WHERE TRUNC(CDATE) = TRUNC(SYSDATE) - 1
          AND A.CNOTE_NO = B.CNOTE_NO(+)
    """

    def get_count(conn_func, query):
        conn = conn_func()
        if not conn:
            return None
        cur = conn.cursor()
        cur.execute(query)
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return count

    count_dbrbn = get_count(get_oracle_connection_dbrbn, query_total)
    count_billing = get_count(get_oracle_connection_billing, query_total)
    count_unbilled = get_count(get_oracle_connection_billing, query_unbilled)
    count_cnote_update = get_count(get_oracle_connection_billing, query_connote_update)

    error_logs = extract_error_from_log(today_date)
    remark_data = {
        "summary": {
            "Reborn": count_dbrbn,
            "Billing": count_billing,
            "Unbilled (BILL_FLAG='N')": count_unbilled
        },
        "errors": error_logs
    }
    remark_json = json.dumps(remark_data, ensure_ascii=False)

    remark = f"Reborn: {count_dbrbn}, Billing: {count_billing}, Unbilled (BILL_FLAG='N'): {count_unbilled}"

    p_monitoring_sync_cnote(
        module='CN',
        total_reborn=count_dbrbn,
        total_billing=count_billing,
        periode=today_date,
        total_bill_flag=count_unbilled,
        remark=remark_json,
        total_cnote_update=count_cnote_update
    )

    return {
        "periode": today_date,
        "total_reborn": count_dbrbn,
        "total_billing": count_billing,
        "unbilled_flag": count_unbilled,
        "total_cnote_update": count_cnote_update,
        "status": "MATCH" if count_dbrbn == count_billing else "NOT MATCH",
        "remark": remark_data
    }
