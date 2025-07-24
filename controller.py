from datetime import datetime
from tqdm import tqdm

from flask import jsonify
from case.connote_update.p_update_cnote_bill_flag import p_update_cnote_bill_flag
from case.connote_update.p_sync_cnote_upd_process import p_sync_cnote_upd_process
from case.connote_update.p_sync_r_cnote_upd_process import p_sync_r_cnote_upd_process
from case.connote_update.p_get_job_cnote_audit import p_get_job_cnote_audit
from db import get_oracle_connection_billing, get_oracle_connection_dbrbn
from case.moda.p_get_bag_no import p_get_bag_no

def get_cnote_numbers():
    # Get the CNOTE numbers from the database or other source
    connection = get_oracle_connection_billing()
    if connection:
        cursor = connection.cursor()
        query = """
                SELECT A.CNOTE_NO
                FROM CMS_CNOTE B,
                     REPJNE.CONNOTE_UPDATE A
                WHERE BILL_FLAG = 'N'
                  AND TRUNC(CDATE) = TRUNC(SYSDATE) - 1
              --    AND TRUNC(B.CNOTE_DATE) < TRUNC(CDATE)
                 AND A.CNOTE_NO = B.CNOTE_NO(+)
                """
        cursor.execute(query)
        cnote_numbers = [row[0] for row in cursor.fetchall()]
        cursor.close()

        # Limit to 1000 CNOTE numbers only
        cnote_numbers = cnote_numbers[:10000]
        if cnote_numbers:
            update_results = []
            errors = []
            for cnote in tqdm(cnote_numbers, desc="Processing CNOTE", unit="item"):
                try:
                    cnote_result = p_sync_cnote_upd_process(cnote, connection)
                    if cnote_result['status'] == "error":
                        print(f"[FAILED] p_sync_cnote_upd_process: {cnote} | Error: {cnote_result.get('message', '')}")
                        raise Exception(f"Failed to update CNOTE: {cnote}")
                    connection.commit()

                    r_cnote_result = p_sync_r_cnote_upd_process(cnote, connection)
                    if r_cnote_result['status'] == "error":
                        print(f"[FAILED] p_sync_r_cnote_upd_process: {cnote} | Error: {r_cnote_result.get('message', '')}")
                        raise Exception(f"Failed to update R_CNOTE: {cnote}")
                    connection.commit()

                    update_flag_result = p_update_cnote_bill_flag(cnote, connection)
                    if update_flag_result['status'] == "error":
                        print(f"[FAILED] p_update_cnote_bill_flag: {cnote} | Error: {update_flag_result.get('message', '')}")
                        raise Exception(f"Failed to update Bill Flag for CNOTE: {cnote}")
                    connection.commit()

                    get_job_cnote_audit = p_get_job_cnote_audit(cnote, connection)
                    if get_job_cnote_audit['status'] == "error":
                        print(f"[FAILED] p_get_job_cnote_audit: {cnote} | Error: {get_job_cnote_audit.get('error', '')}")
                        print(f"Audit failed or no audit result for CNOTE: {cnote}. Skipping audit.")
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
            return jsonify({"message": "Tidak ada nomor CNOTE yang ditemukan atau terjadi kesalahan pada query!"}), 500
    else:
        return jsonify({"message": "Tidak dapat terhubung ke database Billing."}), 500



def get_moda(p_date):
    # Get the moda (BAG_NO and TRANSIT_MANIFEST) data from the database
    connection = get_oracle_connection_dbrbn()
    if connection:
        cursor = connection.cursor()
        query = """
                SELECT BAG_NO, TRANSIT_MANIFEST
                FROM JNE.CMS_COST_TRANSIT_V2
                WHERE TRUNC(TRANSIT_MANIFEST_DATE) = TO_DATE(:p_date, 'YYYY-MM-DD')
                  and bag_no = 'AA64126057'
                GROUP BY BAG_NO, TRANSIT_MANIFEST 
                """

        cursor.execute(query, {'p_date': p_date})
        moda_data = cursor.fetchall()
        cursor.close()

        if moda_data:
            from case.moda.p_get_bag_no import p_get_bag_no
            from case.moda.p_upd_cost import p_upd_cost
            p_get_bag_no_count = 0
            p_upd_cost_count = 0
            results = []
            for bag_no, transit_manifest in moda_data:
                # Proses p_get_bag_no
                result_bag = p_get_bag_no(bag_no, transit_manifest, p_date)
                if result_bag.get('status') == 'success':
                    p_get_bag_no_count += 1
                # Proses p_upd_cost
                result_upd = p_upd_cost(bag_no, transit_manifest)
                if result_upd.get('status') == 'success':
                    p_upd_cost_count += 1
                results.append({
                    "BAG_NO": bag_no,
                    "TRANSIT_MANIFEST": transit_manifest,
                    "p_get_bag_no_result": result_bag,
                    "p_upd_cost_result": result_upd
                })
            summary = {
                "p_get_bag_no": p_get_bag_no_count,
                "p_upd_cost": p_upd_cost_count
            }
            return jsonify({"Moda Data": results, "summary": summary}), 200
        else:
            return jsonify({"message": "No data found for the provided date."}), 404
    else:
        return jsonify({"message": "Unable to connect to DBRBN database."}), 500
