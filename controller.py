from datetime import datetime
from tqdm import tqdm
from flask import jsonify
import logging

from case.connote_update.p_update_cnote_bill_flag import p_update_cnote_bill_flag
from case.connote_update.p_sync_cnote_upd_process import p_sync_cnote_upd_process
from case.connote_update.p_sync_r_cnote_upd_process import p_sync_r_cnote_upd_process
from case.connote_update.p_get_job_cnote_audit import p_get_job_cnote_audit
from db import get_oracle_connection_billing, get_oracle_connection_dbrbn
from case.moda.p_get_bag_no import p_get_bag_no

logger = logging.getLogger(__name__)

# Fungsi untuk mendapatkan CNOTE Numbers dan melakukan proses sync
def get_cnote_numbers(job_id):
    # Generate unique job ID to trace the task
    logger.info(f"Job ID {job_id}: Starting to fetch CNOTE numbers...")

    connection = get_oracle_connection_billing()
    if connection:
        cursor = connection.cursor()
        query = """
                SELECT A.CNOTE_NO
                FROM CMS_CNOTE B,
                     REPJNE.CONNOTE_UPDATE A
                WHERE BILL_FLAG = 'N'
                  AND TRUNC(CDATE) = TRUNC(SYSDATE) - 1
                  AND A.CNOTE_NO = B.CNOTE_NO(+)
                """
        cursor.execute(query)
        cnote_numbers = [row[0] for row in cursor.fetchall()]
        cursor.close()

        # Cek apakah data CNOTE ada (limit 10 ribu)
        if len(cnote_numbers) == 0:
            logger.info(f"Job ID {job_id}: No CNOTE found for processing.")
            return jsonify({"message": "No CNOTE numbers found."}), 404  # Tidak ada data untuk diproses

        # Batasi hanya 10.000 CNOTE untuk diambil
        cnote_numbers = cnote_numbers[:1000]

        # Proses CNOTE numbers
        update_results = []
        errors = []
        for cnote in tqdm(cnote_numbers, desc="Processing CNOTE", unit="item"):
            try:
                cnote_result = p_sync_cnote_upd_process(cnote, connection)
                if cnote_result['status'] == "error":
                    logger.error(f"Job ID {job_id}: Failed to update CNOTE {cnote}. Error: {cnote_result.get('message', '')}")
                    raise Exception(f"Failed to update CNOTE: {cnote}")
                connection.commit()

                r_cnote_result = p_sync_r_cnote_upd_process(cnote, connection)
                if r_cnote_result['status'] == "error":
                    logger.error(f"Job ID {job_id}: Failed to update R_CNOTE {cnote}. Error: {r_cnote_result.get('message', '')}")
                    raise Exception(f"Failed to update R_CNOTE: {cnote}")
                connection.commit()

                update_flag_result = p_update_cnote_bill_flag(cnote, connection)
                if update_flag_result['status'] == "error":
                    logger.error(f"Job ID {job_id}: Failed to update Bill Flag for CNOTE {cnote}. Error: {update_flag_result.get('message', '')}")
                    raise Exception(f"Failed to update Bill Flag for CNOTE: {cnote}")
                connection.commit()

                get_job_cnote_audit = p_get_job_cnote_audit(cnote, connection)
                if get_job_cnote_audit['status'] == "error":
                    logger.error(f"Job ID {job_id}: Audit failed or no audit result for CNOTE {cnote}. Skipping audit.")
                else:
                    update_results.append({"CNOTE": cnote, "Audit_Result": get_job_cnote_audit})

                update_results.append({
                    "CNOTE": cnote,
                    "CNOTE_Update_Result": cnote_result,
                    "R_CNOTE_Update_Result": r_cnote_result,
                    "Update_Flag_Result": update_flag_result
                })

            except Exception as e:
                errors.append({"CNOTE": cnote, "error": str(e)})

        if update_results:
            percent = int((len(update_results) / len(cnote_numbers)) * 100)
            return jsonify({
                "CNOTE Numbers": cnote_numbers,
                "Sync Results": update_results,
                "Progress": f"{percent}%"
            }), 200

        return jsonify({"message": "Beberapa CNOTE gagal diperbarui.", "errors": errors, "Progress": f"{int((len(update_results) / len(cnote_numbers)) * 100)}%"}), 500

    else:
        return jsonify({"message": "Tidak dapat terhubung ke database Billing."}), 500
