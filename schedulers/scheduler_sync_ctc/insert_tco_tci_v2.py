import datetime
import calendar
from db import get_oracle_connection_dbrbn
from logger_config import ctc_logger as logger


def sync_run_ctc():
    logger.info("Starting CTC sync process for June - August 2025")

    conn = None
    try:
        logger.debug("Establishing database connection")
        conn = get_oracle_connection_dbrbn()
        cursor = conn.cursor()
        logger.info("Database connection established successfully")

        year = 2025
        start_month = 6  # Juni
        end_month = 8    # Agustus

        for month in range(start_month, end_month + 1):
            # Tentukan tanggal awal & akhir bulan
            start_date = datetime.datetime(year, month, 1)
            last_day = calendar.monthrange(year, month)[1]
            end_date = datetime.datetime(year, month, last_day)
            total_days = (end_date - start_date).days + 1

            logger.info(f"Processing month: {start_date.strftime('%B %Y')} "
                        f"({start_date.strftime('%d %b')} - {end_date.strftime('%d %b')})")

            # --- Step 1: Execute all INSERT procedures ---
            logger.info("Starting execution of JNE.CTC_INSERT_TCO_TCI_V2 for all dates")
            current_date = start_date
            for _ in range(total_days):
                date_str = current_date.strftime('%d %b %Y').upper()
                try:
                    logger.debug(f"Executing JNE.CTC_INSERT_TCO_TCI_V2('{date_str}')")
                    cursor.callproc('JNE.CTC_INSERT_TCO_TCI_V2', [date_str])
                    conn.commit()
                    logger.info(f"Successfully executed and committed CTC_INSERT_TCO_TCI_V2 for {date_str}")
                except Exception as e:
                    logger.error(f"Error executing CTC_INSERT_TCO_TCI_V2 for {date_str}: {str(e)}", exc_info=True)
                    conn.rollback()
                current_date += datetime.timedelta(days=1)

            # --- Step 2: Execute all UPDATE procedures ---
            logger.info("Starting execution of JNE.UPDT_OMX_TCO for all dates")
            current_date = start_date
            for _ in range(total_days):
                date_str = current_date.strftime('%d %b %Y').upper()
                try:
                    logger.debug(f"Executing JNE.UPDT_OMX_TCO('{date_str}')")
                    cursor.callproc('JNE.UPDT_OMX_TCO', [date_str])
                    conn.commit()
                    logger.info(f"Successfully executed and committed UPDT_OMX_TCO for {date_str}")
                except Exception as e:
                    logger.error(f"Error executing UPDT_OMX_TCO for {date_str}: {str(e)}", exc_info=True)
                    conn.rollback()
                current_date += datetime.timedelta(days=1)

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
