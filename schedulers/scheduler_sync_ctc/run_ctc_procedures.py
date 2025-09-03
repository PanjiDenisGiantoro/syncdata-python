import datetime
import calendar
from concurrent.futures import ThreadPoolExecutor
from db import get_oracle_connection_dbrbn
from logger_config import ctc_logger as logger
from logger_config import day_ctc_logger as day_logger


def execute_ctc_procedure(date_str):
    """Execute CTC_UPD_TCO_TCI_V2 procedure for a single date"""
    conn = None
    try:
        conn = get_oracle_connection_dbrbn()
        cursor = conn.cursor()
        cursor.callproc('JNE.CTC_UPD_TCO_TCI_V2', [date_str])
        conn.commit()
        day_logger.info(f"Successfully executed CTC_UPD_TCO_TCI_V2 for {date_str}")
        return (date_str, True, None)
    except Exception as e:
        day_logger.error(f"Error executing CTC_UPD_TCO_TCI_V2 for {date_str}: {str(e)}", exc_info=True)
        if conn:
            conn.rollback()
        return (date_str, False, str(e))
    finally:
        if conn:
            conn.close()


def execute_cost_transit_procedure(date_str):
    """Execute P_UPDATE_COST_TRANSIT procedure for a single date"""
    conn = None
    try:
        conn = get_oracle_connection_dbrbn()
        cursor = conn.cursor()
        cursor.callproc('JNE.P_UPDATE_COST_TRANSIT', [date_str])
        conn.commit()
        day_logger.info(f"Successfully executed P_UPDATE_COST_TRANSIT for {date_str}")
        return (date_str, True, None)
    except Exception as e:
        day_logger.error(f"Error executing P_UPDATE_COST_TRANSIT for {date_str}: {str(e)}", exc_info=True)
        if conn:
            conn.rollback()
        return (date_str, False, str(e))
    finally:
        if conn:
            conn.close()


def process_procedure_batch(dates_batch, procedure_func, procedure_name):
    """Process a batch of dates with the given procedure function"""
    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_date = {
            executor.submit(procedure_func, date_str): date_str
            for date_str in dates_batch
        }

        for future in future_to_date:
            date_str = future_to_date[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                day_logger.error(f"Error in {procedure_name} for {date_str}: {str(e)}", exc_info=True)
                results.append((date_str, False, str(e)))

    return results


def update_tco_tci_v2():
    logger.info("Starting CTC sync process from July - August 2025 (10 dates at a time)")

    try:
        year = 2025
        start_month = 7  # July
        end_month = 8  # August
        batch_size = 10  # Process 10 dates at a time

        # Generate all dates first
        all_dates = []
        for month in range(start_month, end_month + 1):
            start_date = datetime.datetime(year, month, 1)
            last_day = calendar.monthrange(year, month)[1]
            end_date = datetime.datetime(year, month, last_day)

            current_date = start_date
            while current_date <= end_date:
                all_dates.append(current_date.strftime('%d %b %Y').upper())
                current_date += datetime.timedelta(days=1)

        logger.info(f"Total {len(all_dates)} days to process")

        # First, run CTC_UPD_TCO_TCI_V2 for all dates
        logger.info("Starting CTC_UPD_TCO_TCI_V2 execution")
        for i in range(0, len(all_dates), batch_size):
            batch = all_dates[i:i + batch_size]
            logger.info(f"Processing CTC_UPD_TCO_TCI_V2 batch {i // batch_size + 1}: {batch[0]} to {batch[-1]}")

            results = process_procedure_batch(batch, execute_ctc_procedure, "CTC_UPD_TCO_TCI_V2")

            # Log batch results
            success = sum(1 for _, success, _ in results if success)
            failed = len(results) - success
            logger.info(f"CTC_UPD_TCO_TCI_V2 batch completed: {success} succeeded, {failed} failed")

        # Then, run P_UPDATE_COST_TRANSIT for all dates
        logger.info("Starting P_UPDATE_COST_TRANSIT execution")
        for i in range(0, len(all_dates), batch_size):
            batch = all_dates[i:i + batch_size]
            logger.info(f"Processing P_UPDATE_COST_TRANSIT batch {i // batch_size + 1}: {batch[0]} to {batch[-1]}")

            results = process_procedure_batch(batch, execute_cost_transit_procedure, "P_UPDATE_COST_TRANSIT")

            # Log batch results
            success = sum(1 for _, success, _ in results if success)
            failed = len(results) - success
            logger.info(f"P_UPDATE_COST_TRANSIT batch completed: {success} succeeded, {failed} failed")

        logger.info("CTC procedures execution completed")
        return True

    except Exception as e:
        logger.critical(f"Fatal error in CTC sync process: {str(e)}", exc_info=True)
        return False