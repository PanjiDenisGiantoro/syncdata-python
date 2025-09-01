import datetime
import calendar
from db import get_oracle_connection_dbrbn
from logger_config import ctc_logger as logger

def update_tco_tci_v2():
    """
    Execute the JNE.CTC_UPD_TCO_TCI_V2 and P_UPDATE_COST_TRANSIT stored procedures
    for each day in the specified date range.
    """
    logger.info("Starting CTC sync process from 24 July - August 2025")
    
    conn = None
    try:
        logger.debug("Establishing database connection")
        conn = get_oracle_connection_dbrbn()
        cursor = conn.cursor()
        logger.info("Database connection established successfully")

        year = 2025
        start_month = 7  # Juli
        end_month = 8    # Agustus

        for month in range(start_month, end_month + 1):
            # Determine start and end dates for the month
            start_date = datetime.datetime(year, month, 1)  # Always start from 1st of the month
            last_day = calendar.monthrange(year, month)[1]
            end_date = datetime.datetime(year, month, last_day)
            total_days = (end_date - start_date).days + 1

            logger.info(f"Processing month: {start_date.strftime('%B %Y')} "
                      f"({start_date.strftime('%d %b')} - {end_date.strftime('%d %b')})")

            # Process each day in the date range
            current_date = start_date
            for _ in range(total_days):
                date_str = current_date.strftime('%d %b %Y').upper()
                
                try:
                    # Execute CTC_UPD_TCO_TCI_V2 procedure with date parameter
                    logger.debug(f"Executing JNE.CTC_UPD_TCO_TCI_V2('{date_str}')")
                    cursor.callproc('JNE.CTC_UPD_TCO_TCI_V2', [date_str])
                    conn.commit()
                    logger.info(f"Successfully executed and committed CTC_UPD_TCO_TCI_V2 for {date_str}")
                    
                    # Execute P_UPDATE_COST_TRANSIT procedure with date parameter
                    logger.debug(f"Executing P_UPDATE_COST_TRANSIT('{date_str}')")
                    cursor.callproc('P_UPDATE_COST_TRANSIT', [date_str])
                    conn.commit()
                    logger.info(f"Successfully executed and committed P_UPDATE_COST_TRANSIT for {date_str}")
                    
                except Exception as e:
                    logger.error(f"Error executing procedures for {date_str}: {str(e)}", exc_info=True)
                    conn.rollback()
                    
                current_date += datetime.timedelta(days=1)

        logger.info("CTC procedures execution completed successfully")
        return True
        
    except Exception as e:
        logger.critical(f"Fatal error in CTC sync process: {str(e)}", exc_info=True)
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
