from datetime import datetime
from tqdm import tqdm
from flask import jsonify

from case.connote_update.p_count_cnote import p_count_cnote
from logger_config import logger
from progress_utils import save_progress, load_progress, clear_progress
import requests

# Initialize progress_data from file
progress_data = load_progress()
from case.cms_mflight.p_sync_flight import p_sync_flight
from case.connote_update.p_update_cnote_bill_flag import p_update_cnote_bill_flag
from case.connote_update.p_sync_cnote_upd_process import p_sync_cnote_upd_process
from case.connote_update.p_sync_r_cnote_upd_process import p_sync_r_cnote_upd_process
from case.connote_update.p_get_job_cnote_audit import p_get_job_cnote_audit
from db import get_oracle_connection_billing, get_oracle_connection_dbrbn
from case.connote_update.p_monitoring_data_cnote import monitoring_cnote_count_today

progress_data = None  # Will be set by app.py



def get_flight(job_id):
    """
    Fetches flight data from AviationStack API and processes it.

    Args:
        job_id (str): Unique identifier for the job

    Returns:
        dict: Processing results and status
    """
    logger.info(f"Job ID {job_id}: Starting to fetch flight data...")

    # API Configuration
    base_url = "https://api.aviationstack.com/v1/timetable"
    params = {
        'iataCode': 'CGK',
        'type': 'departure',
        'access_key': '4a75614d656449337e99fac724ffc997'
    }

    try:
        # Make API request
        logger.info(f"Job ID {job_id}: Fetching data from AviationStack API...")
        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Raise exception for HTTP errors

        # Parse JSON response
        data = response.json()

        # Check if data was returned successfully
        if not data.get('data'):
            logger.warning(f"Job ID {job_id}: No flight data found in API response")
            return {
                "status": "completed",
                "message": "No flight data available",
                "processed": 0
            }

        # Process the flight data
        logger.info(f"Job ID {job_id}: Processing {len(data['data'])} flight records...")
        result = p_sync_flight(data['data'])

        logger.info(f"Job ID {job_id}: Successfully processed flight data")
        return {
            "status": "completed",
            "message": "Flight data processed successfully",
            "processed": result.get('processed', 0),
            "details": result
        }

    except requests.exceptions.RequestException as e:
        error_msg = f"Error fetching flight data: {str(e)}"
        logger.error(f"Job ID {job_id}: {error_msg}")
        return {
            "status": "error",
            "message": error_msg,
            "processed": 0
        }
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(f"Job ID {job_id}: {error_msg}")
        return {
            "status": "error",
            "message": error_msg,
            "processed": 0
        }

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
                  FETCH FIRST 100000 ROWS ONLY
                """
        cursor.execute(query)
        cnote_numbers = [row[0] for row in cursor.fetchall()]
        cursor.close()

        if len(cnote_numbers) == 0:
            logger.info(f"Job ID {job_id}: No CNOTE found for processing.")
            progress_data.update({'total': 0, 'success': 0, 'failed': 0, 'status': 'Selesai'})
            save_progress(progress_data)
            return jsonify({"message": "No CNOTE numbers found."}), 404

        batch_size = 500
        total_records = len(cnote_numbers)
        total_batches = (total_records + batch_size - 1) // batch_size
        success_count = 0
        failed_count = 0
        batch_group_size = 10  # Process 10 batches at once
        batch_groups = []
        current_batch = []

        # Group batches
        for i in range(0, total_records, batch_size):
            batch = cnote_numbers[i:i + batch_size]
            current_batch.append(batch)
            if len(current_batch) >= batch_group_size or i + batch_size >= total_records:
                batch_groups.append(current_batch)
                current_batch = []

        # Initialize progress_data
        progress_data.update({
            'total': total_records,
            'total_batches': total_batches,
            'total_groups': len(batch_groups),
            'current_batch': 0,
            'current_group': 0,
            'success': 0,
            'failed': 0,
            'status': 'Processing',
            'logs': []
        })
        save_progress(progress_data)

        logger.info(f"Job ID {job_id}: Starting to process {total_records} CNOTE numbers in {len(batch_groups)} groups")

        try:
            for group_idx, batch_group in enumerate(batch_groups, 1):
                progress_data.update({
                    'current_group': group_idx,
                    'current_batch': min(group_idx * batch_group_size, total_batches)
                })
                
                group_size = sum(len(batch) for batch in batch_group)
                logger.info(f"Job ID {job_id}: Processing group {group_idx}/{len(batch_groups)} with {len(batch_group)} batches ({group_size} records)")

                try:
                    # Process all batches in the group
                    all_batches = [batch for batch in batch_group if batch]  # Remove any empty batches
                    
                    # Process p_sync_cnote_upd_process with all batches
                    logger.info(f"Job ID {job_id}: Running p_sync_cnote_upd_process for group {group_idx}...")
                    p_sync_cnote_upd_process(*all_batches)

                    # Process p_sync_r_cnote_upd_process with all batches
                    logger.info(f"Job ID {job_id}: Running p_sync_r_cnote_upd_process for group {group_idx}...")
                    p_sync_r_cnote_upd_process(*all_batches)

                    # # Process p_get_job_cnote_audit for all batches in the group
                    logger.info(f"Job ID {job_id}: Running p_get_job_cnote_audit for group {group_idx}...")
                    p_get_job_cnote_audit(*all_batches)

                    # # Update cnote bill flag for all batches in the group
                    logger.info(f"Job ID {job_id}: Running p_update_cnote_bill_flag for group {group_idx}...")
                    p_update_cnote_bill_flag(*all_batches)

                    # Update success count for all batches in the group
                    group_success = sum(len(batch) for batch in batch_group)
                    success_count += group_success
                    progress_data.update({'success': success_count})
                    progress_data['logs'].append(f"✅ Processed {len(batch_group)} batches in group {group_idx} ({group_success} records)")
                    save_progress(progress_data)

                    # print(f" CNOTE no : {all_batches}")
                    logger.info(f"Job ID {job_id}: Successfully processed group {group_idx}/{len(batch_groups)}")


                except Exception as group_error:
                    # If group processing fails, process each batch individually
                    logger.error(f"Job ID {job_id}: Error in group {group_idx} processing, falling back to individual batch processing: {str(group_error)}")
                    progress_data['logs'].append(f"❌ Error in group {group_idx} processing, falling back to individual batch processing: {str(group_error)}")
                    save_progress(progress_data)

        except Exception as e:
            logger.error(f"Job ID {job_id}: Unexpected error in batch processing: {str(e)}")
            progress_data.update({'status': 'Gagal'})
            save_progress(progress_data)
            return jsonify({
                "error": f"Unexpected error in batch processing: {str(e)}",
                "processed": success_count,
                "failed": failed_count
            }), 500
