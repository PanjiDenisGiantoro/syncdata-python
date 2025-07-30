import oracledb
from config import Config
from db import get_oracle_connection_billing

def p_get_job_cnote_audit(p_cnote):
    try:
        connection = get_oracle_connection_billing()
        if connection:
            cursor = connection.cursor()
            try:
                cursor.callproc("JOB_CNOTE_AUDIT_NEW", [p_cnote])
            except Exception as e:
                return {"status": "error", "error": str(e)}
            return {"status": "success", "info": f"JOB_CNOTE_AUDIT_NEW called for {p_cnote}"}
    finally:
            connection.close()