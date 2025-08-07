import oracledb
from config import Config
from db import get_oracle_connection_dbrbn, get_oracle_connection_billing
from logger_config import logger


def process_cnote_batch(cursor, moda_batch):
    """Process a single batch of MODA records"""
    if not moda_batch:
        return 0, 0

    try:
        # Create a list of tuples with the unique identifiers for each record
        records = [(row[2], row[3], row[4]) for row in moda_batch]  # (TRANSIT_MANIFEST_NO, BAG_NO, SMU_NUMBER)

        # Create a list of bind variables for the IN clause
        bind_vars = {}
        in_clauses = []

        for i, (manifest_no, bag_no, smu_no) in enumerate(records):
            bind_vars.update({
                f"manifest_no_{i}": manifest_no,
                f"bag_no_{i}": bag_no,
                f"smu_no_{i}": smu_no
            })
            in_clauses.append(f"(:manifest_no_{i}, :bag_no_{i}, :smu_no_{i})")

        print(in_clauses)
        # Build the UPDATE query with IN clause
        update_query = f"""
        UPDATE JNE.CMS_COST_TRANSIT_V2@DBRBN t
        SET MODA_TYPE_DESC = F_GET_MODA_TYPE_DESC(t.BAG_NO, t.TRANSIT_MANIFEST)
        WHERE (t.TRANSIT_MANIFEST_NO, t.BAG_NO, t.SMU_NUMBER) IN ({', '.join(in_clauses)})
        """

        cursor.execute(update_query, bind_vars)
        return len(records), 0

    except Exception as e:
        logger.error(f"Error processing batch of {len(moda_batch)} MODA records: {str(e)}")
        return 0, len(moda_batch)

def p_upd_moda(*batch_groups):
        all_moda = [moda for batch in batch_groups for moda in (batch if isinstance(batch, list) else [batch])]
        if not all_moda:
            return {"status": "error", "message": "No MODA records provided"}

        try:
            connection = get_oracle_connection_billing()
            if not connection:
                return {"status": "error", "message": "Failed to connect to database"}

            try:
                cursor = connection.cursor()
                total_processed = 0
                total_failed = 0
                batch_size = 500  # Process 500 records at a time

                # Process in batches
                for i in range(0, len(all_moda), batch_size):
                    batch = all_moda[i:i + batch_size]
                    try:
                        success, failed = process_cnote_batch(cursor, batch)
                        total_processed += success
                        total_failed += failed
                        connection.commit()  # Commit after each successful batch

                        logger.info(
                            f"Processed batch {i // batch_size + 1}/{(len(all_moda) + batch_size - 1) // batch_size}: "
                            f"{success} succeeded, {failed} failed")

                    except Exception as batch_error:
                        connection.rollback()
                        logger.error(f"Error processing batch starting at index {i}: {str(batch_error)}")
                        total_failed += len(batch)
                        # Continue with next batch even if one fails

                return {
                    "status": "success",
                    "message": f"Processed {total_processed} MODA records successfully, {total_failed} failed",
                    "processed": total_processed,
                    "failed": total_failed
                }

            except Exception as e:
                connection.rollback()
                logger.error(f"Error in p_upd_moda: {str(e)}")
                return {
                    "status": "error",
                    "message": f"Failed to process MODA records: {str(e)}",
                    "processed": total_processed,
                    "failed": total_failed if 'total_failed' in locals() else len(all_moda)
                }

            finally:
                cursor.close()
                connection.close()

        except Exception as e:
            logger.error(f"Unexpected error in p_upd_moda: {str(e)}")
            return {
                "status": "error",
                "message": f"Unexpected error in p_upd_moda: {str(e)}",
                "processed": total_processed,
                "failed": total_failed if 'total_failed' in locals() else len(all_moda)
            }
