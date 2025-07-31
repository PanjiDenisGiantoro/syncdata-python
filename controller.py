from datetime import datetime
from tqdm import tqdm
from flask import jsonify
from logger_config import logger
from progress_utils import save_progress, load_progress, clear_progress

# Initialize progress_data from file
progress_data = load_progress()

from case.connote_update.p_update_cnote_bill_flag import p_update_cnote_bill_flag
from case.connote_update.p_sync_cnote_upd_process import p_sync_cnote_upd_process
from case.connote_update.p_sync_r_cnote_upd_process import p_sync_r_cnote_upd_process
from case.connote_update.p_get_job_cnote_audit import p_get_job_cnote_audit
from db import get_oracle_connection_billing
from case.connote_update.p_monitoring_data_cnote import monitoring_cnote_count_today

progress_data = None  # Will be set by app.py


def get_cnote_numbers(job_id):
    global progress_data
    # Initialize fresh progress data
    progress_data = {
        'total': 0,
        'success': 0,
        'failed': 0,
        'status': 'Menunggu',
        'batch_size': 2,
        'total_batches': 0,
        'current_batch': 0,
        'logs': []
    }
    save_progress(progress_data)

    logger.info(f"Job ID {job_id}: Starting to fetch CNOTE numbers...")
    progress_data.update({'total': 0, 'success': 0, 'failed': 0, 'status': 'Sedang Berjalan'})
    save_progress(progress_data)

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
                  FETCH FIRST 10000 ROWS ONLY
                """
        cursor.execute(query)
        cnote_numbers = [row[0] for row in cursor.fetchall()]
        cursor.close()

        if len(cnote_numbers) == 0:
            logger.info(f"Job ID {job_id}: No CNOTE found for processing.")
            progress_data.update({'total': 0, 'success': 0, 'failed': 0, 'status': 'Selesai'})
            save_progress(progress_data)
            return jsonify({"message": "No CNOTE numbers found."}), 404

        batch_size = 1000
        total_records = len(cnote_numbers)
        total_batches = (total_records + batch_size - 1) // batch_size
        success_count = 0
        failed_count = 0

        # Initialize progress_data
        progress_data.update({
            'total': total_records,
            'success': 0,
            'failed': 0,
            'status': 'Sedang Berjalan',
            'batch_size': batch_size,
            'total_batches': total_batches,
            'current_batch': 0,
            'logs': []
        })
        save_progress(progress_data)

        logger.info(f"Job ID {job_id}: Starting to process {total_records} CNOTE numbers in batches of {batch_size}...")
        print(cnote_numbers)

        try:
            for i in range(0, total_records, batch_size):
                batch = cnote_numbers[i:i + batch_size]
                batch_number = (i // batch_size) + 1
                progress_data.update({'current_batch': batch_number})
                progress_percent_batch = (batch_number / total_batches) * 100

                logger.info(f"Job ID {job_id}: Processing batch {batch_number}/{total_batches} with {len(batch)} records")

                try:
                    logger.info(f"Job ID {job_id}: Running p_sync_cnote_upd_process for batch {batch_number}...")
                    p_sync_cnote_upd_process(batch)

                    logger.info(f"Job ID {job_id}: Running p_sync_r_cnote_upd_process for batch {batch_number}...")
                    p_sync_r_cnote_upd_process(batch)

                    logger.info(f"Job ID {job_id}: Running p_update_cnote_bill_flag for batch {batch_number}...")
                    p_update_cnote_bill_flag(batch)

                    logger.info(f"Job ID {job_id}: Running p_get_job_cnote_audit for batch {batch_number}...")
                    p_get_job_cnote_audit(batch)

                    success_count += len(batch)
                    progress_data.update({'success': success_count})
                    progress_data['logs'].append(f"✅ Batch {batch_number}: {len(batch)} records processed successfully")
                    save_progress(progress_data)

                    logger.info(f"Job ID {job_id}: Successfully processed batch {batch_number}/{total_batches} "
                                f"({progress_percent_batch:.2f}%)")
                except Exception as batch_error:
                    failed_count += len(batch)
                    progress_data.update({'failed': failed_count})
                    progress_data['logs'].append(f"❌ Batch {batch_number}: Failed - {str(batch_error)}")
                    save_progress(progress_data)
                    logger.error(f"Job ID {job_id}: Error in batch {batch_number}: {str(batch_error)}")
                    continue

            progress_data.update({'status': 'Selesai', 'current_batch': total_batches})
            save_progress(progress_data)
            logger.info(f"Job ID {job_id}: Batch processing completed. Success: {success_count}, Failed: {failed_count}")

            if failed_count == 0:
                return jsonify({
                    "message": f"Successfully processed {success_count} CNOTE numbers in batches.",
                    "processed": success_count,
                    "failed": failed_count,
                    "progress": "100%"
                }), 200
            else:
                return jsonify({
                    "message": f"Processed with some failures. Success: {success_count}, Failed: {failed_count}",
                    "processed": success_count,
                    "failed": failed_count,
                    "progress": f"{(progress_data['current_batch'] / progress_data['total_batches']) * 100:.2f}%"
                }), 207
        except Exception as e:
            logger.error(f"Job ID {job_id}: Unexpected error in batch processing: {str(e)}")
            progress_data.update({'status': 'Gagal'})
            save_progress(progress_data)
            return jsonify({
                "error": f"Unexpected error in batch processing: {str(e)}",
                "processed": success_count,
                "failed": failed_count
            }), 500
