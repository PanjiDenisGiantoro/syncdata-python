import datetime
import calendar
from db import get_oracle_connection_billing
from logger_config import ctcexample_logger as logger

def sync_dci():
    logger.info("Starting CTC sync process for Jun 2025")
    conn = None
    BATCH_SIZE = 10 # Process 20 dates in each batch

    try:
        logger.debug("Establishing database connection")
        conn = get_oracle_connection_billing()
        cursor = conn.cursor()
        logger.info("Database connection established successfully")
        year = 2025
        start_month = 6  # April
        end_month = 7  # May
        procedure_calls = []
        # First, collect all the procedure calls
        for month in range(start_month, end_month + 1):
            start_date = datetime.datetime(year, month, 1)
            last_day = calendar.monthrange(year, month)[1]
            end_date = datetime.datetime(year, month, last_day)
            current_date = start_date

            logger.info(f"Preparing procedure calls for {start_date.strftime('%B %Y')}")

            while current_date <= end_date:
                date_str = current_date.strftime('%d %b %Y').upper()
                procedure_calls.append(date_str)
                current_date += datetime.timedelta(days=1)

        # Process in batches
        total_calls = len(procedure_calls)
        for i in range(0, total_calls, BATCH_SIZE):
            batch = procedure_calls[i:i + BATCH_SIZE]
            batch_number = (i // BATCH_SIZE) + 1
            total_batches = (total_calls + BATCH_SIZE - 1) // BATCH_SIZE
            logger.info(f"Processing batch {batch_number} of {total_batches} (dates: {batch[0]} to {batch[-1]})")
            try:
                for date_str in batch:
                    cursor.callproc('JNEBILL.INS_CTC_MAY_MONTH',[date_str])
                    logger.debug(f"Executed JNEBILL.INS_CTC_MAY_MONTH('{date_str}')")
                conn.commit()
                logger.info(f"Successfully committed batch {batch_number} of {total_batches}")

            except Exception as e:
                conn.rollback()
                logger.error(f"Error during batch {batch_number} execution: {str(e)}", exc_info=True)
                continue

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