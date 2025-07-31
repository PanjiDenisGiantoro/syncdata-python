import oracledb
from config import Config
from db import get_oracle_connection_billing

def p_get_job_cnote_audit(p_cnote_list):
    results = []

    try:
        connection = get_oracle_connection_billing()
        if connection:
            cursor = connection.cursor()
            for cnote in p_cnote_list:
                try:
                    cursor.callproc("JOB_CNOTE_AUDIT_NEW", [cnote])
                    results.append({"cnote": cnote, "status": "success"})
                except Exception as e:
                    results.append({"cnote": cnote, "status": "error", "error": str(e)})
    finally:
        if connection:
            connection.close()

    return results
