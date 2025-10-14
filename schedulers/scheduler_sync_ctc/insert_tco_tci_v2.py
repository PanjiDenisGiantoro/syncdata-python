import datetime
import calendar
from db import get_oracle_connection_dbrbn,get_oracle_connection_ctcv2db
from logger_config import ctc_logger as logger
from concurrent.futures import ThreadPoolExecutor, as_completed



BATCH_SIZE = 30  # jumlah tanggal yang akan dijalankan bersamaan


def call_sync(date_str):
    """Fungsi untuk memanggil prosedur INSERT Oracle per tanggal."""
    conn = None
    try:
        conn = get_oracle_connection_dbrbn()
        cursor = conn.cursor()
        cursor.callproc('JNE.CTC_INSERT_TCO_TCI_V2_RDR', [date_str])
        conn.commit()
        logger.info(f"[INSERT] Executed {date_str} successfully")
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"[INSERT] Error executing {date_str}: {str(e)}", exc_info=True)
    finally:
        if conn:
            conn.close()


def call_insert(date_str):
    """Fungsi untuk memanggil prosedur INSERT Oracle per tanggal."""
    conn = None
    try:
        conn = get_oracle_connection_dbrbn()
        cursor = conn.cursor()
        cursor.callproc('JNE.CTC_INSERT_TCO_TCI_V2_RDR', [date_str])
        conn.commit()
        logger.info(f"[INSERT] Executed {date_str} successfully")
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"[INSERT] Error executing {date_str}: {str(e)}", exc_info=True)
    finally:
        if conn:
            conn.close()




def call_update(date_str):
    """Fungsi untuk memanggil prosedur UPDATE Oracle per tanggal."""
    conn = None
    try:
        conn = get_oracle_connection_dbrbn()
        cursor = conn.cursor()
        cursor.callproc('JNE.UPDT_OMX_TCO_RDR', [date_str])
        conn.commit()
        logger.info(f"[UPDATE] Executed {date_str} successfully")
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"[UPDATE] Error executing {date_str}: {str(e)}", exc_info=True)
    finally:
        if conn:
            conn.close()


def call_sync(date_str):
    """Fungsi untuk memanggil prosedur SYNC Oracle per tanggal."""
    conn = None
    try:
        conn = get_oracle_connection_ctcv2db()
        cursor = conn.cursor()
        cursor.callproc('sync_cost_transit_v2', [date_str])
        conn.commit()
        logger.info(f"[SYNC] Executed {date_str} successfully")
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"[SYNC] Error executing {date_str}: {str(e)}", exc_info=True)
    finally:
        if conn:
            conn.close()


def sync_run_ctc_insert():
    logger.info("Starting CTC sync process for Januari - Des 2024 (batch 30)")

    try:
        year = 2025
        start_month = 9  # April
        end_month = 9   # Desember

        procedure_calls = []
        # Kumpulkan semua tanggal
        for month in range(start_month, end_month + 1):
            # if month == 2:
            #     start_date = datetime.datetime(year, month, 9)
            # else:
            #     start_date = datetime.datetime(year, month, 1)
            start_date = datetime.datetime(year, month, 1)
            last_day = calendar.monthrange(year, month)[1]
            end_date = datetime.datetime(year, month, last_day)
            current_date = start_date

            while current_date <= end_date:
                date_str = current_date.strftime('%d %b %Y').upper()
                procedure_calls.append(date_str)
                current_date += datetime.timedelta(days=1)

        total_calls = len(procedure_calls)
        total_batches = (total_calls + BATCH_SIZE - 1) // BATCH_SIZE

        # --- Step 1: INSERT procedure ---
        logger.info("Starting Step 1: sync_cost_transit_v2 in batches")

        for i in range(0, total_calls, BATCH_SIZE):
            batch = procedure_calls[i:i + BATCH_SIZE]
            batch_number = (i // BATCH_SIZE) + 1
            logger.info(f"[INSERT] Processing batch {batch_number}/{total_batches} "
                        f"({batch[0]} to {batch[-1]})")

            with ThreadPoolExecutor(max_workers=BATCH_SIZE) as executor:
                futures = [executor.submit(call_sync, date_str) for date_str in batch]
                for future in as_completed(futures):
                    # Hanya untuk memastikan semua task selesai
                    future.result()

            logger.info(f"[SYNC] Completed batch {batch_number}/{total_batches}")

        logger.info("CTC sync process completed successfully")

    except Exception as e:
        logger.critical(f"Fatal error in CTC sync process: {str(e)}", exc_info=True)
        raise




def sync_run_ctc():
    logger.info("Starting CTC sync process for Januari - Des 2024 (batch 30)")

    try:
        year = 2025
        start_month = 9  # April
        end_month = 9   # Desember

        procedure_calls = []
        # Kumpulkan semua tanggal
        for month in range(start_month, end_month + 1):
            # if month == 4:
            #     start_date = datetime.datetime(year, month, 18)
            # else:
            start_date = datetime.datetime(year, month, 1)

            last_day = calendar.monthrange(year, month)[1]
            end_date = datetime.datetime(year, month, last_day)
            current_date = start_date

            while current_date <= end_date:
                date_str = current_date.strftime('%d %b %Y').upper()
                procedure_calls.append(date_str)
                current_date += datetime.timedelta(days=1)

        total_calls = len(procedure_calls)
        total_batches = (total_calls + BATCH_SIZE - 1) // BATCH_SIZE

        # --- Step 1: INSERT procedure ---
        logger.info("Starting Step 1: JNE.CTC_INSERT_TCO_TCI_V2_RDR in batches")

        for i in range(0, total_calls, BATCH_SIZE):
            batch = procedure_calls[i:i + BATCH_SIZE]
            batch_number = (i // BATCH_SIZE) + 1
            logger.info(f"[INSERT] Processing batch {batch_number}/{total_batches} "
                        f"({batch[0]} to {batch[-1]})")

            with ThreadPoolExecutor(max_workers=BATCH_SIZE) as executor:
                futures = [executor.submit(call_insert, date_str) for date_str in batch]
                for future in as_completed(futures):
                    # Hanya untuk memastikan semua task selesai
                    future.result()

            logger.info(f"[INSERT] Completed batch {batch_number}/{total_batches}")

        # --- Step 2: UPDATE procedure ---
        logger.info("Starting Step 2: JNE.UPDT_OMX_TCO in batches")

        for i in range(0, total_calls, BATCH_SIZE):
            batch = procedure_calls[i:i + BATCH_SIZE]
            batch_number = (i // BATCH_SIZE) + 1
            logger.info(f"[UPDATE] Processing batch {batch_number}/{total_batches} "
                        f"({batch[0]} to {batch[-1]})")

            with ThreadPoolExecutor(max_workers=BATCH_SIZE) as executor:
                futures = [executor.submit(call_update, date_str) for date_str in batch]
                for future in as_completed(futures):
                    future.result()

            logger.info(f"[UPDATE] Completed batch {batch_number}/{total_batches}")

        logger.info("CTC sync process completed successfully")

    except Exception as e:
        logger.critical(f"Fatal error in CTC sync process: {str(e)}", exc_info=True)
        raise


def sync_run_ctc_dev():
    logger.info("Starting CTC sync process for Januari - DES 2024 (batch 10)")

    conn = None
    BATCH_SIZE = 30 # eksekusi 10 tanggal sekaligus

    try:
        logger.debug("Establishing database connection")
        conn = get_oracle_connection_dbrbn()
        cursor = conn.cursor()
        logger.info("Database connection established successfully")

        year = 2024
        start_month = 4  # Januari
        end_month = 12    # Des
        
        procedure_calls = []
        # --- Kumpulkan semua tanggal ---
        for month in range(start_month, end_month + 1):
            if month == 4:  # Maret mulai tanggal 21
                start_date = datetime.datetime(year, month, 17)
            else:           # Bulan lain mulai tanggal 1
                start_date = datetime.datetime(year, month, 1)
            # start_date = datetime.datetime(year, month, 1)
            last_day = calendar.monthrange(year, month)[1]
            end_date = datetime.datetime(year, month, last_day)
            current_date = start_date

            logger.info(f"Preparing procedure calls for {start_date.strftime('%B %Y')}")

            while current_date <= end_date:
                date_str = current_date.strftime('%d %b %Y').upper()
                procedure_calls.append(date_str)
                current_date += datetime.timedelta(days=1)

        total_calls = len(procedure_calls)
        total_batches = (total_calls + BATCH_SIZE - 1) // BATCH_SIZE

        # --- Step 1: INSERT procedure ---
        logger.info("Starting Step 1: JNE.CTC_INSERT_TCO_TCI_V2_RDR in batches")
        for i in range(0, total_calls, BATCH_SIZE):
            batch = procedure_calls[i:i + BATCH_SIZE]
            batch_number = (i // BATCH_SIZE) + 1
            logger.info(f"[INSERT] Processing batch {batch_number}/{total_batches} "
                        f"({batch[0]} to {batch[-1]})")

            try:
                for date_str in batch:
                    cursor.callproc('JNE.CTC_INSERT_TCO_TCI_V2_RDR', [date_str])
                    logger.debug(f"Executed JNE.CTC_INSERT_TCO_TCI_V2_RDR('{date_str}')")

                conn.commit()
                logger.info(f"[INSERT] Successfully committed batch {batch_number}/{total_batches}")

            except Exception as e:
                conn.rollback()
                logger.error(f"[INSERT] Error in batch {batch_number}: {str(e)}", exc_info=True)

        # --- Step 2: UPDATE procedure ---
        logger.info("Starting Step 2: JNE.UPDT_OMX_TCO in batches")
        for i in range(0, total_calls, BATCH_SIZE):
            batch = procedure_calls[i:i + BATCH_SIZE]
            batch_number = (i // BATCH_SIZE) + 1
            logger.info(f"[UPDATE] Processing batch {batch_number}/{total_batches} "
                        f"({batch[0]} to {batch[-1]})")

            try:
                for date_str in batch:
                    cursor.callproc('JNE.UPDT_OMX_TCO_RDR', [date_str])
                    logger.debug(f"Executed JNE.UPDT_OMX_TCO('{date_str}')")

                conn.commit()
                logger.info(f"[UPDATE] Successfully committed batch {batch_number}/{total_batches}")

            except Exception as e:
                conn.rollback()
                logger.error(f"[UPDATE] Error in batch {batch_number}: {str(e)}", exc_info=True)

    except Exception as e:
        logger.critical(f"Fatal error in CTC sync process: {str(e)}", exc_info=True)
        raise

    finally:
        if conn:
            try:
                conn.close()
                logger.info("Database connection closed")
            except Exception as e:
                logger.error(f"Error during connection close: {str(e)}", exc_info=True)

        logger.info("CTC sync process completed")



def sync_run_ctc_day():
    logger.info("Starting daily CTC sync process for yesterday")

    conn = None
    try:
        logger.debug("Establishing database connection")
        conn = get_oracle_connection_dbrbn()
        cursor = conn.cursor()
        logger.info("Database connection established successfully")

        # Get yesterday's date
        yesterday = datetime.datetime.now() - datetime.timedelta(days=2)
        date_str = yesterday.strftime('%d %b %Y').upper()

        logger.info(f"Processing date: {date_str}")

        try:
            logger.debug(f"Executing JNE.CTC_INSERT_TCO_TCI_V2_RDR('{date_str}')")
            cursor.callproc('JNE.CTC_INSERT_TCO_TCI_V2_RDR', [date_str])
            conn.commit()
            logger.info(f"Successfully executed and committed CTC_INSERT_TCO_TCI_V2_RDR for {date_str}")

            logger.debug(f"Executing JNE.UPDT_OMX_TCO_RDR('{date_str}')")
            cursor.callproc('JNE.UPDT_OMX_TCO_RDR', [date_str])
            conn.commit()
            logger.info(f"Successfully executed and committed UPDT_OMX_TCO_RDR for {date_str}")

            logger.debug(f"Executing JNE.CTC_UPD_TCO_TCI_V3_RDR('{date_str}')")
            cursor.callproc('JNE.CTC_UPD_TCO_TCI_V3_RDR', [date_str])
            conn.commit()
            logger.info(f"Successfully executed and committed CTC_UPD_TCO_TCI_V3_RDR for {date_str}")

            logger.debug(f"Executing P_UPDATE_COST_TRANSIT('{date_str}')")
            cursor.callproc('JNE.P_UPDATE_COST_TRANSIT', [date_str])
            conn.commit()
            logger.info(f"Successfully executed and committed P_UPDATE_COST_TRANSIT for {date_str}")
            try:
                # First, delete existing data for the date if it exists
                delete_sql = """
                             DELETE \
                             FROM cms_cost_transit_v2@ctcv2db
                             WHERE TRUNC(transit_manifest_date) = TO_DATE(:date_str, 'DD MON YYYY') \
                             """
                cursor.execute(delete_sql, date_str=date_str)
                logger.info(f"Deleted existing data for {date_str} from target table")

                # Then insert new data
                insert_sql = """
                             INSERT INTO cms_cost_transit_v2@ctcv2db
                             SELECT * \
                             FROM jne.cms_cost_transit_v2
                             WHERE TRUNC(transit_manifest_date) = TO_DATE(:date_str, 'DD MON YYYY') \
                             """
                cursor.execute(insert_sql, date_str=date_str)
                conn.commit()

                # Log the number of rows inserted
                count_sql = """
                            SELECT COUNT(*)
                            FROM jne.cms_cost_transit_v2
                            WHERE TRUNC(transit_manifest_date) = TO_DATE(:date_str, 'DD MON YYYY') \
                            """
                cursor.execute(count_sql, date_str=date_str)
                row_count = cursor.fetchone()[0]

                logger.info(f"Successfully copied {row_count} rows to cms_cost_transit_v2@ctcv2db for {date_str}")

            except Exception as e:
                logger.error(f"Error copying data to cms_cost_transit_v2@ctcv2db: {str(e)}", exc_info=True)
                if conn:
                    conn.rollback()
                return False

            logger.info(f"Daily CTC sync completed successfully for {date_str}")
            return True

        except Exception as e:
            logger.error(f"Error executing procedures for {date_str}: {str(e)}", exc_info=True)
            if conn:
                conn.rollback()
            return False

    except Exception as e:
        logger.critical(f"Fatal error in daily CTC sync process: {str(e)}", exc_info=True)
        if conn:
            conn.rollback()
        return False

    finally:
        if conn:
            try:
                conn.close()
                logger.info("Database connection closed")
            except Exception as e:
                logger.error(f"Error during connection close: {str(e)}", exc_info=True)

        logger.info("Daily CTC sync process completed")
