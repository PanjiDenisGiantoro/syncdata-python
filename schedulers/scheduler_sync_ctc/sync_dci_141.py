import datetime
import calendar
from concurrent.futures import ThreadPoolExecutor, as_completed
from db import get_oracle_connection_billing
from logger_config import ctc_logger as logger
from logger_config import day_ctc_logger as day_logger


def execute_ins_ctc_to141(date_str):
    """Execute JNEBILL.INS_CTC_TO141 procedure for a single date"""
    conn = None
    try:
        conn = get_oracle_connection_billing()
        cursor = conn.cursor()
        cursor.callproc('JNEBILL.INS_CTC_TO141', [date_str])
        conn.commit()
        day_logger.info(f"Successfully executed JNEBILL.INS_CTC_TO141 for {date_str}")
        return (date_str, True, None)
    except Exception as e:
        day_logger.error(f"Error executing JNEBILL.INS_CTC_TO141 for {date_str}: {str(e)}", exc_info=True)
        if conn:
            conn.rollback()
        return (date_str, False, str(e))
    finally:
        if conn:
            conn.close()


def sync_dci_141():
    logger.info("Starting sync DCI 141 process from Jan - Apr 2025 (20 dates per batch)")

    try:
        year = 2024
        start_month = 2  # Januari
        end_month = 2   # April
        batch_size = 20
        max_workers = 10

        # Generate all dates
        all_dates = []
        for month in range(start_month, end_month + 1):
            start_date = datetime.datetime(year, month, 1)
            last_day = calendar.monthrange(year, month)[1]
            end_date = datetime.datetime(year, month, last_day)

            current_date = start_date
            while current_date <= end_date:
                all_dates.append(current_date.strftime('%d %b %Y').upper())
                current_date += datetime.timedelta(days=1)

        logger.info(f"Total {len(all_dates)} days to process for DCI 141")

        # --- Step: INS_CTC_TO141 ---
        logger.info("Starting INS_CTC_TO141 execution")
        for i in range(0, len(all_dates), batch_size):
            batch = all_dates[i:i + batch_size]
            batch_number = (i // batch_size) + 1
            logger.info(f"[DCI141] Processing batch {batch_number}: {batch[0]} to {batch[-1]}")

            results = []
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(execute_ins_ctc_to141, d): d for d in batch}
                for future in as_completed(futures):
                    try:
                        results.append(future.result())
                    except Exception as e:
                        day_logger.error(
                            f"Unexpected error in DCI141 batch {batch_number}: {str(e)}",
                            exc_info=True
                        )
                        results.append((futures[future], False, str(e)))

            success = sum(1 for _, ok, _ in results if ok)
            failed = len(results) - success
            logger.info(f"[DCI141] Batch {batch_number} completed: {success} succeeded, {failed} failed")

        logger.info("Sync DCI 141 process completed successfully")
        return True

    except Exception as e:
        logger.critical(f"Fatal error in DCI 141 sync process: {str(e)}", exc_info=True)
        return False
