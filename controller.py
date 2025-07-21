from datetime import datetime

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

        if cnote_numbers:
            # Sync each CNOTE number with the update process
            update_results = []
            errors = []  # Store errors separately
            for cnote in cnote_numbers:
                print(f"Syncing CNOTE: {cnote}")
                try:
                    # Start a transaction for multiple operations
                    # Call the CNOTE update process
                    cnote_result = p_sync_cnote_upd_process(cnote)
                    if cnote_result['status'] == "error":
                        raise Exception(f"Failed to update CNOTE: {cnote}")
                    # Commit after the CNOTE update
                    connection.commit()

                    # Call the R_CNOTE update process
                    r_cnote_result = p_sync_r_cnote_upd_process(cnote)
                    if r_cnote_result['status'] == "error":
                        raise Exception(f"Failed to update R_CNOTE: {cnote}")
                    # Commit after the R_CNOTE update
                    connection.commit()

                    # Call the Bill Flag update process
                    update_flag_result = p_update_cnote_bill_flag(cnote)
                    if update_flag_result['status'] == "error":
                        raise Exception(f"Failed to update Bill Flag for CNOTE: {cnote}")
                    # Commit after the Bill Flag update
                    connection.commit()

                    # Call the job audit process
                    get_job_cnote_audit = p_get_job_cnote_audit(cnote)

                    if get_job_cnote_audit['status'] == "error":
                        # Jika audit gagal atau tidak ada hasil, tetap commit
                        print(f"Audit failed or no audit result for CNOTE: {cnote}. Skipping audit.")
                    else:
                        # Jika audit berhasil, simpan hasilnya
                        update_results.append(
                            {"CNOTE": cnote, "Audit_Result": get_job_cnote_audit}
                        )

                    # Commit setelah semua proses (meskipun audit gagal atau tidak ada hasil)
                    connection.commit()

                    # Jika semua operasi berhasil, catat hasilnya
                    update_results.append(
                        {"CNOTE": cnote,
                         "CNOTE_Update_Result": cnote_result,
                         "R_CNOTE_Update_Result": r_cnote_result,
                         "Update_Flag_Result": update_flag_result
                         }
                    )

                except Exception as e:
                    # Log error dan lanjutkan ke CNOTE berikutnya
                    errors.append({"CNOTE": cnote, "error": str(e)})

            # Jika semua operasi berhasil, kembalikan hasil
            if update_results:
                return jsonify({"CNOTE Numbers": cnote_numbers, "Sync Results": update_results}), 200

            # Jika ada error, kembalikan pesan error
            return jsonify({"message": "Beberapa CNOTE gagal diperbarui.", "errors": errors}), 500

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
