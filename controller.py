from datetime import datetime
from tqdm import tqdm
from flask import jsonify
from logger_config import logger
# Progress data is now managed in app.py
progress_data = None  # Will be set by app.py

from case.connote_update.p_update_cnote_bill_flag import p_update_cnote_bill_flag
from case.connote_update.p_sync_cnote_upd_process import p_sync_cnote_upd_process
from case.connote_update.p_sync_r_cnote_upd_process import p_sync_r_cnote_upd_process
from case.connote_update.p_get_job_cnote_audit import p_get_job_cnote_audit
from db import get_oracle_connection_billing, get_oracle_connection_dbrbn
from case.connote_update.p_monitoring_data_cnote import monitoring_cnote_count_today # Import fungsi monitoring


progress_data = {
    'total': 0,
    'success': 0,
    'failed': 0
}
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
                  FETCH FIRST 10 ROWS ONLY
                """
        cursor.execute(query)
        cnote_numbers = [row[0] for row in cursor.fetchall()]
        cursor.close()

        # Cek apakah data CNOTE ada (limit 10 ribu)
        if len(cnote_numbers) == 0:
            logger.info(f"Job ID {job_id}: No CNOTE found for processing.")
            return jsonify({"message": "No CNOTE numbers found."}), 404  # Tidak ada data untuk diproses

        # Set maximum limit to 1000 records and process in batches of 100
        batch_size = 2
        
        # Limit to max_records
        # cnote_numbers = cnote_numbers[:max_records]
        total_records = len(cnote_numbers)
        processed_count = 0
        success_count = 0
        failed_count = 0
        print(cnote_numbers)
        # Reset progress data
        if progress_data is not None:
            progress_data['total'] = total_records
            progress_data['success'] = 0
            progress_data['failed'] = 0
            
        try:
            logger.info(f"Job ID {job_id}: Starting to process {total_records} CNOTE numbers in batches of {batch_size}...")
            
            # Process in batches
            for i in range(0, total_records, batch_size):
                batch = cnote_numbers[i:i + batch_size]
                batch_number = (i // batch_size) + 1
                total_batches = (total_records + batch_size - 1) // batch_size
                
                logger.info(f"Job ID {job_id}: Processing batch {batch_number}/{total_batches} with {len(batch)} records")
                
                try:
                    # Process each function sequentially for the current batch
                    logger.info(f"Job ID {job_id}: Starting p_sync_cnote_upd_process for batch {batch_number}...")
                    p_sync_cnote_upd_process(batch)
                    
                    logger.info(f"Job ID {job_id}: Starting p_sync_r_cnote_upd_process for batch {batch_number}...")
                    p_sync_r_cnote_upd_process(batch)
                    
                    logger.info(f"Job ID {job_id}: Starting p_update_cnote_bill_flag for batch {batch_number}...")
                    p_update_cnote_bill_flag(batch)
                    
                    logger.info(f"Job ID {job_id}: Starting p_get_job_cnote_audit for batch {batch_number}...")
                    p_get_job_cnote_audit(batch)
                    
                    # Update success count for the batch
                    success_count += len(batch)
                    if progress_data is not None:
                        progress_data['success'] = success_count
                        progress_data['failed'] = failed_count
                    
                    logger.info(f"Job ID {job_id}: Successfully processed batch {batch_number}/{total_batches} with {len(batch)} records")
                    
                except Exception as batch_error:
                    failed_count += len(batch)
                    if progress_data is not None:
                        progress_data['failed'] = failed_count
                    logger.error(f"Job ID {job_id}: Error in batch {batch_number}: {str(batch_error)}")
                    # Continue with next batch even if one fails
                    continue
            
            # Final status
            logger.info(f"Job ID {job_id}: Batch processing completed. Success: {success_count}, Failed: {failed_count}")
            
            if failed_count == 0:
                return jsonify({
                    "message": f"Successfully processed {success_count} CNOTE numbers in batches.",
                    "processed": success_count,
                    "failed": failed_count
                }), 200
            else:
                return jsonify({
                    "message": f"Processed with some failures. Success: {success_count}, Failed: {failed_count}",
                    "processed": success_count,
                    "failed": failed_count
                }), 207  # 207 Multi-Status
                
        except Exception as e:
            logger.error(f"Job ID {job_id}: Unexpected error in batch processing: {str(e)}")
            return jsonify({
                "error": f"Unexpected error in batch processing: {str(e)}",
                "processed": success_count,
                "failed": failed_count
            }), 500