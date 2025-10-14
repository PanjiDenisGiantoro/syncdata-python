import datetime
import calendar
import oracledb
from concurrent.futures import ThreadPoolExecutor, as_completed
from db import get_oracle_connection_dbrbn, get_oracle_connection_ctcv2db
from logger_config import ctc_logger as logger
from logger_config import day_ctc_logger as day_logger
from config import Config

# Connection Pool Setup
pool = None

def get_connection_pool():
    """Initialize and return connection pool"""
    global pool
    if pool is None:
        try:
            pool = oracledb.SessionPool(
                user=Config.DB_USER_CTCV2,
                password=Config.DB_PASSWORD_CTCV2,
                dsn=Config.DB_DSN_CTCV2,
                min=3,      # Minimum connections
                max=8,       # Maximum connections (5 + buffer)
                increment=2,  # Increment step
                timeout=60,   # Connection timeout
                retry_count=3 # Retry count
            )
            logger.info("Connection pool initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize connection pool: {str(e)}", exc_info=True)
            raise
    return pool

def close_connection_pool():
    """Close connection pool"""
    global pool
    if pool:
        try:
            pool.close()
            pool = None
            logger.info("Connection pool closed successfully")
        except Exception as e:
            logger.error(f"Error closing connection pool: {str(e)}", exc_info=True)


def execute_ctc_procedure(date_str):
    """Execute CTC_UPD_TCO_TCI_V2_MODA_V2 procedure for a single date"""
    conn = None
    try:
        # Get connection from pool
        pool = get_connection_pool()
        conn = pool.acquire()
        cursor = conn.cursor()
        cursor.callproc('CTC_UPD_TCO_TCI_V2_MODA_V2', [date_str])
        conn.commit()
        day_logger.info(f"Successfully executed CTC_UPD_TCO_TCI_V2_MODA_V2 for {date_str}")
        return (date_str, True, None)
    except Exception as e:
        day_logger.error(f"Error executing CTC_UPD_TCO_TCI_V2_MODA_V2 for {date_str}: {str(e)}", exc_info=True)
        if conn:
            conn.rollback()
        return (date_str, False, str(e))
    finally:
        if conn and pool:
            pool.release(conn)


def execute_cost_transit_procedure(date_str):
    """Execute P_UPDATE_COST_TRANSIT_V2 procedure for a single date"""
    conn = None
    try:
        # Get connection from pool
        pool = get_connection_pool()
        conn = pool.acquire()
        cursor = conn.cursor()
        cursor.callproc('P_UPDATE_COST_TRANSIT_V2', [date_str])
        conn.commit()
        day_logger.info(f"Successfully executed P_UPDATE_COST_TRANSIT_V2 for {date_str}")
        return (date_str, True, None)
    except Exception as e:
        day_logger.error(f"Error executing P_UPDATE_COST_TRANSIT_V2 for {date_str}: {str(e)}", exc_info=True)
        if conn:
            conn.rollback()
        return (date_str, False, str(e))
    finally:
        if conn and pool:
            pool.release(conn)


def execute_insert_cost_transit(date_str):
    """Insert into cms_cost_transit_v2@ctcv2db from jne.cms_cost_transit_v2 for a single date"""
    conn = None
    try:
        conn = get_oracle_connection_dbrbn()
        cursor = conn.cursor()
        sql = """
            INSERT INTO cms_cost_transit_v2@ctcv2db
            SELECT *
            FROM jne.cms_cost_transit_v2
            WHERE TRUNC(transit_manifest_date) = TO_DATE(:date_str, 'DD MON YYYY')
        """
        cursor.execute(sql, {"date_str": date_str})
        conn.commit()
        day_logger.info(f"Successfully executed INSERT cms_cost_transit_v2 for {date_str}")
        return (date_str, True, None)
    except Exception as e:
        day_logger.error(f"Error executing INSERT cms_cost_transit_v2 for {date_str}: {str(e)}", exc_info=True)
        if conn:
            conn.rollback()
        return (date_str, False, str(e))
    finally:
        if conn:
            conn.close()

def execute_ctc_update_btbpbd(start_date_str, end_date_str):
    """Execute DBCTC_V2.P_CTC_UPDATE_BTBPBD procedure for a date range"""
    conn = None
    try:
        conn = get_oracle_connection_ctcv2db()
        cursor = conn.cursor()
        cursor.callproc('DBCTC_V2.P_CTC_UPDATE_BTBPBD', [start_date_str, end_date_str])
        conn.commit()
        day_logger.info(f"Successfully executed DBCTC_V2.P_CTC_UPDATE_BTBPBD for range {start_date_str} to {end_date_str}")
        return (start_date_str, end_date_str, True, None)
    except Exception as e:
        day_logger.error(f"Error executing DBCTC_V2.P_CTC_UPDATE_BTBPBD for range {start_date_str} to {end_date_str}: {str(e)}", exc_info=True)
        if conn:
            conn.rollback()
        return (start_date_str, end_date_str, False, str(e))
    finally:
        if conn:
            conn.close()

def execute_ctc_update_btbpbd(start_date_str, end_date_str):
    """Execute DBCTC_V2.P_CTC_UPDATE_BTBPBD procedure for a date range"""
    conn = None
    try:
        conn = get_oracle_connection_ctcv2db()
        cursor = conn.cursor()
        cursor.callproc('DBCTC_V2.P_CTC_UPDATE_BTBPBD', [start_date_str, end_date_str])
        conn.commit()
        day_logger.info(f"Successfully executed DBCTC_V2.P_CTC_UPDATE_BTBPBD for range {start_date_str} to {end_date_str}")
        return (start_date_str, end_date_str, True, None)
    except Exception as e:
        day_logger.error(f"Error executing DBCTC_V2.P_CTC_UPDATE_BTBPBD for range {start_date_str} to {end_date_str}: {str(e)}", exc_info=True)
        if conn:
            conn.rollback()
        return (start_date_str, end_date_str, False, str(e))
    finally:
        if conn:
            conn.close()


def execute_upd_stg_charge_bag(file_name):
    """Execute PROC_UPD_REP_STG_CHARGE_BAG_NF procedure for a single file"""
    conn = None
    try:
        conn = get_oracle_connection_ctcv2db()
        cursor = conn.cursor()
        cursor.callproc('PROC_UPD_REP_STG_CHARGE_BAG_NF', [file_name])
        conn.commit()
        logger.info(f"Successfully executed PROC_UPD_REP_STG_CHARGE_BAG_NF for {file_name}")
        return (file_name, True, None)
    except Exception as e:
        logger.error(f"Error executing PROC_UPD_REP_STG_CHARGE_BAG_NF for {file_name}: {str(e)}", exc_info=True)
        if conn:
            conn.rollback()
        return (file_name, False, str(e))
    finally:
        if conn:
            conn.close()

def update_rep_stg_charge_bag():
    """Main function to process PROC_UPD_REP_STG_CHARGE_BAG212 for all relevant files"""
    logger.info("Starting REP_STG_CHARGE_BAG update process")

    try:
        # Step 1: Get list of file names to process
        conn = get_oracle_connection_ctcv2db()
        cursor = conn.cursor()

        query = """
                        SELECT DISTINCT(stg_file_name)
                FROM rep_stg_charge
                WHERE stg_file_name IN ('03 MAR 2025.csv','04 APR 2025.csv','05 MAY 2025.csv','06 JUN 2025.csv','07 JUL 2025.csv') 
                or  stg_file_name like '%blankBAG-2024-01%'
                or stg_file_name like '%blankBAG-2024-02%'
                ORDER BY stg_file_name ASC
        """
        cursor.execute(query)
        file_names = [row[0] for row in cursor.fetchall()]
        conn.close()

        if not file_names:
            logger.warning("No file names found to process.")
            return False

        logger.info(f"Found {len(file_names)} file(s) to process")

        # Step 2: Execute the procedure for each file in parallel
        batch_size = 10  # Process 10 files at a time in each batch
        max_workers = 10  # Max parallel workers

        results = []
        for i in range(0, len(file_names), batch_size):
            batch = file_names[i:i + batch_size]
            batch_number = (i // batch_size) + 1
            logger.info(f"[STG] Processing batch {batch_number}: {batch[0]} to {batch[-1]}")

            # Step 3: Execute each task in parallel using ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(execute_upd_stg_charge_bag, file_name): file_name for file_name in batch}
                for future in as_completed(futures):
                    try:
                        results.append(future.result())  # Append results to the list
                    except Exception as e:
                        logger.error(f"Unexpected error in batch bags {batch_number}: {str(e)}", exc_info=True)
                        results.append((futures[future], False, str(e)))

            # Logging batch results
            success = sum(1 for _, ok, _ in results if ok)
            failed = len(results) - success
            logger.info(f"[STG] Batch {batch_number} completed: {success} succeeded, {failed} failed")

        # Final summary
        total_batches = len(results)
        success_count = sum(1 for _, ok, _ in results if ok)
        failed_count = total_batches - success_count
        logger.info(f"[STG] All batches completed: {success_count} succeeded, {failed_count} failed out of {total_batches}")

        if failed_count > 0:
            logger.warning("REP_STG_CHARGE_BAG update process completed with some failures. Check logs for details.")
            return False
        else:
            logger.info("REP_STG_CHARGE_BAG update process completed successfully")
            return True

    except Exception as e:
        logger.critical(f"Fatal error in REP_STG_CHARGE_BAG update process: {str(e)}", exc_info=True)
        return False

def update_btbpbd():
    logger.info("Starting BTBPBD update process for Feb 2025 (batched by date ranges)")

    try:
        year = 2024
        start_month = 1  # Februari (1=Januari, 2=Februari)
        end_month = 6   # Februari (sesuaikan jika perlu, e.g., 4 untuk April)
        batch_size = 10
        max_workers = 10

        # Generate all dates untuk periode
        all_dates = []
        for month in range(start_month, end_month + 1):
            start_date = datetime.datetime(year, month, 1)
            last_day = calendar.monthrange(year, month)[1]
            end_date = datetime.datetime(year, month, last_day)

            current_date = start_date
            while current_date <= end_date:
                all_dates.append(current_date.strftime('%d %b %Y').upper())
                current_date += datetime.timedelta(days=1)

        logger.info(f"Total {len(all_dates)} days to process in {len(all_dates)//batch_size + (1 if len(all_dates) % batch_size else 0)} batches")

        # Bagi menjadi batch sub-range dan jalankan prosedur untuk setiap range secara parallel
        results = []
        for i in range(0, len(all_dates), batch_size):
            batch = all_dates[i:i + batch_size]
            if not batch:
                continue
            batch_number = (i // batch_size) + 1
            start_range = batch[0]
            end_range = batch[-1]
            logger.info(f"[BTBPBD] Processing batch {batch_number}: range {start_range} to {end_range} ({len(batch)} days)")

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Karena setiap batch adalah satu range, submit satu tugas per batch
                future = executor.submit(execute_ctc_update_btbpbd, start_range, end_range)
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    day_logger.error(f"Unexpected error in BTBPBD batch {batch_number}: {str(e)}", exc_info=True)
                    results.append((start_range, end_range, False, str(e)))

            # Log per batch
            start_str, end_str, success, error = results[-1]
            if success:
                logger.info(f"[BTBPBD] Batch {batch_number} succeeded for {start_str} to {end_str}")
            else:
                logger.error(f"[BTBPBD] Batch {batch_number} failed for {start_str} to {end_str}: {error}")

        # Ringkasan keseluruhan
        total_batches = len(results)
        success_count = sum(1 for _, _, ok, _ in results if ok)
        failed_count = total_batches - success_count
        logger.info(f"[BTBPBD] All batches completed: {success_count} succeeded, {failed_count} failed out of {total_batches}")

        if failed_count > 0:
            logger.warning("BTBPBD process completed with some failures. Check logs for details.")
            # Optional: return False jika ingin strict (semua harus sukses)
            # return False
        else:
            logger.info("BTBPBD update process completed successfully")

        return True

    except Exception as e:
        logger.critical(f"Fatal error in BTBPBD update process: {str(e)}", exc_info=True)
        return False

def update_tco_tci_v2():
    try:
        year = 2024
        start_month = 3
        end_month = 3
        batch_size = 5
        max_workers = 5
        
        # Dynamic logging message
        month_names = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun", 7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"}
        
        start_month_name = month_names.get(start_month, f"Month {start_month}")
        end_month_name = month_names.get(end_month, f"Month {end_month}")
        
        logger.info(f"Starting CTC sync process from {start_month_name} - {end_month_name} {year}")

        # Generate all dates
        all_dates = []
        for month in range(start_month, end_month + 1):
            # if month == 2:
            #     start_date = datetime.datetime(year, month, 1)
            # else:
            #     start_date = datetime.datetime(year, month, 1)

            start_date = datetime.datetime(year, month, 1)
            last_day = calendar.monthrange(year, month)[1]
            end_date = datetime.datetime(year, month, last_day)

            current_date = start_date
            while current_date <= end_date:
                all_dates.append(current_date.strftime('%d %b %Y').upper())
                current_date += datetime.timedelta(days=1)

        logger.info(f"Total {len(all_dates)} days to process")

        # --- Step 1: CTC_UPD_TCO_TCI_V2_MODA_V2 ---
        logger.info("Starting Step 1: CTC_UPD_TCO_TCI_V2_MODA_V2 execution")
        for i in range(0, len(all_dates), batch_size):
            batch = all_dates[i:i + batch_size]
            batch_number = (i // batch_size) + 1
            logger.info(f"[CTC] Processing batch {batch_number}: {batch[0]} to {batch[-1]}")

            results = []
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(execute_ctc_procedure, d): d for d in batch}
                for future in as_completed(futures):
                    try:
                        results.append(future.result())
                    except Exception as e:
                        day_logger.error(f"Unexpected error in CTC batch {batch_number}: {str(e)}", exc_info=True)
                        results.append((futures[future], False, str(e)))

            success = sum(1 for _, ok, _ in results if ok)
            failed = len(results) - success
            logger.info(f"[CTC] Batch {batch_number} completed: {success} succeeded, {failed} failed")

        # --- Step 2: P_UPDATE_COST_TRANSIT_V2 ---
        # logger.info("Starting Step 2: P_UPDATE_COST_TRANSIT_V2 execution")
        # for i in range(0, len(all_dates), batch_size):
        #     batch = all_dates[i:i + batch_size]
        #     batch_number = (i // batch_size) + 1
        #     logger.info(f"[TRANSIT] Processing batch {batch_number}: {batch[0]} to {batch[-1]}")

        #     results = []
        #     with ThreadPoolExecutor(max_workers=max_workers) as executor:
        #         futures = {executor.submit(execute_cost_transit_procedure, d): d for d in batch}
        #         for future in as_completed(futures):
        #             try:
        #                 results.append(future.result())
        #             except Exception as e:
        #                 day_logger.error(f"Unexpected error in TRANSIT batch {batch_number}: {str(e)}", exc_info=True)
        #                 results.append((futures[future], False, str(e)))

            success = sum(1 for _, ok, _ in results if ok)
            failed = len(results) - success
            logger.info(f"[TRANSIT] Batch {batch_number} completed: {success} succeeded, {failed} failed")

        # --- Step 3: INSERT cms_cost_transit_v2@ctcv2db ---
        # logger.info("Starting Step 3: INSERT INTO cms_cost_transit_v2@ctcv2db execution")
        # for i in range(0, len(all_dates), batch_size):
        #     batch = all_dates[i:i + batch_size]
        #     batch_number = (i // batch_size) + 1
        #     logger.info(f"[INSERT] Processing batch {batch_number}: {batch[0]} to {batch[-1]}")

        #     results = []
        #     with ThreadPoolExecutor(max_workers=max_workers) as executor:
        #         futures = {executor.submit(execute_insert_cost_transit, d): d for d in batch}
        #         for future in as_completed(futures):
        #             try:
        #                 results.append(future.result())
        #             except Exception as e:
        #                 day_logger.error(f"Unexpected error in INSERT batch {batch_number}: {str(e)}", exc_info=True)
        #                 results.append((futures[future], False, str(e)))

        #     success = sum(1 for _, ok, _ in results if ok)
        #     failed = len(results) - success
        #     logger.info(f"[INSERT] Batch {batch_number} completed: {success} succeeded, {failed} failed")

        logger.info("CTC sync process completed successfully (Step 1 + Step 2 )")
        return True

    except Exception as e:
        logger.critical(f"Fatal error in CTC sync process: {str(e)}", exc_info=True)
        return False
    finally:
        # Cleanup connection pool
        close_connection_pool()


# Cleanup function untuk shutdown graceful
def cleanup_resources():
    """Cleanup all resources including connection pool"""
    try:
        close_connection_pool()
        logger.info("All resources cleaned up successfully")
    except Exception as e:
        logger.error(f"Error during cleanup: {str(e)}", exc_info=True)
