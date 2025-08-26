import datetime
from db import get_oracle_connection_dbrbn
from logger_config import ctc_logger as logger


def sync_run_ctc():
    logger.info("Starting CTC sync process for June 2025")

    conn = None
    try:
        logger.debug("Establishing database connection")
        conn = get_oracle_connection_dbrbn()
        cursor = conn.cursor()
        logger.info("Database connection established successfully")

        start_date = datetime.datetime(2025, 6, 1)
        end_date = datetime.datetime(2025, 6, 30)  # June has 30 days
        logger.info(
            f"Processing period: {start_date.strftime('%d %b %Y')} "
            f"to {end_date.strftime('%d %b %Y')}"
        )

        current_date = start_date
        total_days = (end_date - start_date).days + 1

        for day in range(total_days):
            date_str = current_date.strftime('%d %b %Y').upper()
            logger.info(f"Processing date: {date_str} ({day + 1}/{total_days})")

            try:
                logger.debug(f"Executing JNE.CTC_INSERT_TCO_TCI_V2 for {date_str}")
                cursor.callproc('JNE.CTC_INSERT_TCO_TCI_V2', [date_str])
                logger.info(f"Successfully executed JNE.CTC_INSERT_TCO_TCI_V2 for {date_str}")

                logger.debug(f"Executing JNE.UPDT_OMX_TCO for {date_str}")
                cursor.callproc('JNE.UPDT_OMX_TCO', [date_str])
                logger.info(f"Successfully executed JNE.UPDT_OMX_TCO for {date_str}")

                conn.commit()
                logger.info(f"Successfully committed changes for {date_str}")

            except Exception as e:
                error_msg = f"Error processing date {date_str}: {str(e)}"
                logger.error(error_msg, exc_info=True)
                conn.rollback()
                logger.info(f"Rolled back changes for {date_str} due to error")
                continue

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
