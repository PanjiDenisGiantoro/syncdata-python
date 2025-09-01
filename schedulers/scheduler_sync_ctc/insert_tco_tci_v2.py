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


def sync_run_ctc_day():
    logger.info("Starting daily CTC sync process for yesterday")

    conn = None
    try:
        logger.debug("Establishing database connection")
        conn = get_oracle_connection_dbrbn()
        cursor = conn.cursor()
        logger.info("Database connection established successfully")

        # Get yesterday's date
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        date_str = yesterday.strftime('%d %b %Y').upper()

        logger.info(f"Processing date: {date_str}")

        try:
            logger.debug(f"Executing JNE.CTC_INSERT_TCO_TCI_V2('{date_str}')")
            cursor.callproc('JNE.CTC_INSERT_TCO_TCI_V2', [date_str])
            conn.commit()
            logger.info(f"Successfully executed and committed CTC_INSERT_TCO_TCI_V2 for {date_str}")

            logger.debug(f"Executing JNE.UPDT_OMX_TCO('{date_str}')")
            cursor.callproc('JNE.UPDT_OMX_TCO', [date_str])
            conn.commit()
            logger.info(f"Successfully executed and committed UPDT_OMX_TCO for {date_str}")

            logger.debug(f"Executing JNE.CTC_UPD_TCO_TCI_V2('{date_str}')")
            cursor.callproc('JNE.CTC_UPD_TCO_TCI_V2', [date_str])
            conn.commit()
            logger.info(f"Successfully executed and committed CTC_UPD_TCO_TCI_V2 for {date_str}")

            logger.debug(f"Executing P_UPDATE_COST_TRANSIT('{date_str}')")
            cursor.callproc('P_UPDATE_COST_TRANSIT', [date_str])
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
