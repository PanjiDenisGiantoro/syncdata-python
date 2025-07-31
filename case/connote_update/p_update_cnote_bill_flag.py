import oracledb
from config import Config
from case.connote_update import p_update_cnote_bill_flag
from db import get_oracle_connection_dbrbn, get_oracle_connection_billing

def p_update_cnote_bill_flag(p_cnote_list):

    if not p_cnote_list:
        return {"status": "error", "error": "No CNOTE numbers provided."}
    try:
        connection = get_oracle_connection_billing()
        if not connection:
            return {"status": "error", "message": "Failed to connect to billing database"}

        try:
            cursor = connection.cursor()

            bind_vars = {f"cnote_{i}": cnote for i, cnote in enumerate(p_cnote_list)}

            in_clause = ", ".join(f":cnote_{i}" for i in range(len(p_cnote_list)))

            merge_query = f"""
            UPDATE REPJNE.CONNOTE_UPDATE
                             SET BILL_FLAG = 'Y'
                             WHERE CNOTE_NO in ({in_clause})
                             """

            cursor.execute(merge_query, bind_vars)
            connection.commit()
            return {"status": "success", "message": "Bill flag updated successfully."}
        except Exception as e:
                connection.rollback()
                return {
                    "status": "error",
                    "message": f"Gagal memproses p_sync_cnote_upd_process batch: {str(e)}",
                    "failed": len(p_cnote_list)
                }

        finally:
            connection.close()
    except Exception as e:
        return {"status": "error", "error": str(e)}