import oracledb
from config import Config
from db import get_oracle_connection_billing
from logger_config import logger
from typing import List, Tuple, Dict, Any, Optional


def process_cnote_batch(cursor, cnotes_batch):
    """Process a single batch of CNOTEs (max 1000 items)"""
    if not cnotes_batch:
        return 0, 0

    # Create parameters for the query
    bind_vars = {f"cnote_{i}": cnote for i, cnote in enumerate(cnotes_batch)}
    in_clause = ", ".join(f":cnote_{i}" for i in range(len(cnotes_batch)))
    merge_query = f"""
        DECLARE
        v_list T_CNOTE_LIST := T_CNOTE_LIST({in_clause});
        BEGIN
            JNEBILL.JOB_CNOTE_AUDIT_NEW_BATCH(v_list);
        EXCEPTION
            WHEN OTHERS THEN
                -- Log the error and re-raise
                DBMS_OUTPUT.PUT_LINE('Error in JOB_CNOTE_AUDIT_NEW_BATCH: ' || SQLERRM);
                RAISE;
        END;
 """
    try:
        cursor.execute(merge_query, bind_vars)
        # print(cnotes_batch)
        return len(cnotes_batch), 0
    except Exception as e:
        logger.error(f"Error processing batch of {len(cnotes_batch)} CNOTEs: {str(e)}")
        return 0, len(cnotes_batch)


def p_get_job_cnote_audit(*batch_groups):
    # Flatten the list of batches into a single list of CNOTEs
    all_cnotes = [cnote for batch in batch_groups for cnote in (batch if isinstance(batch, list) else [batch])]
    if not all_cnotes:
        return {"status": "error", "message": "Tidak ada nomor CNOTE yang diberikan"}

    try:
        connection = get_oracle_connection_billing()
        if not connection:
            return {"status": "error", "message": "Gagal terhubung ke database"}

        try:
            cursor = connection.cursor()
            total_processed = 0
            total_failed = 0
            batch_size = 1000  # Slightly below 1000 to be safe

            # Process in chunks of batch_size
            for i in range(0, len(all_cnotes), batch_size):
                batch = all_cnotes[i:i + batch_size]
                processed, failed = process_cnote_batch(cursor, batch)
                total_processed += processed
                total_failed += failed

                # Commit after each batch to avoid large transactions
                connection.commit()

            return {
                "status": "success" if total_failed == 0 else "partial",
                "message": f"Berhasil memproses {total_processed} CNOTEs" +
                           (f", gagal {total_failed} CNOTEs" if total_failed > 0 else ""),
                "processed": total_processed,
                "failed": total_failed,
                "batches_processed": len(batch_groups)
            }

            # Commit any remaining transactions
            connection.commit()

            return {
                "status": "success" if total_failed == 0 else "partial",
                "message": f"Berhasil memproses p_sync_cnote_upd_process{total_processed} CNOTEs" +
                           (f", gagal {total_failed} CNOTEs" if total_failed > 0 else ""),
                "processed": total_processed,
                "failed": total_failed,
                "batches_processed": len(batch_groups)
            }

        except Exception as e:
            connection.rollback()
            logger.error(f"Gagal memproses p_sync_cnote_upd_process CNOTE: {str(e)}")
            return {
                "status": "error",
                "message": f"Gagal memproses batch: {str(e)}",
                "failed": len(all_cnotes),
                "batches_failed": len(batch_groups)
            }

        finally:
            connection.close()

    except Exception as e:
        logger.error(f"Kesalahan tak terduga: {str(e)}")
        return {
            "status": "error",
            "message": f"Kesalahan: {str(e)}"
        }

