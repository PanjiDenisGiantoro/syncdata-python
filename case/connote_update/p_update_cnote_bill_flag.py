import oracledb
from config import Config
from case.connote_update import p_update_cnote_bill_flag
from db import get_oracle_connection_dbrbn, get_oracle_connection_billing
from logger_config import logger


def process_cnote_batch(cursor, cnotes_batch):
    if not cnotes_batch:
        return 0, 0

    # Create parameters for the query
    bind_vars = {f"cnote_{i}": cnote for i, cnote in enumerate(cnotes_batch)}
    in_clause = ", ".join(f":cnote_{i}" for i in range(len(cnotes_batch)))
    merge_query = f"""
               UPDATE REPJNE.CONNOTE_UPDATE
                                SET BILL_FLAG = 'Y'
                                WHERE CNOTE_NO in ({in_clause})
                                """
    try:
        cursor.execute(merge_query, bind_vars)
        return len(cnotes_batch), 0
    except Exception as e:
        logger.error(f"Error processing batch of {len(cnotes_batch)} CNOTEs: {str(e)}")
        return 0, len(cnotes_batch)

def p_update_cnote_bill_flag(*batch_groups):
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
                "message": f"Berhasil memproses fill_flag{total_processed} CNOTEs" +
                           (f", gagal {total_failed} CNOTEs" if total_failed > 0 else ""),
                "processed": total_processed,
                "failed": total_failed,
                "batches_processed": len(batch_groups)
            }

            # Commit any remaining transactions
            connection.commit()

            return {
                "status": "success" if total_failed == 0 else "partial",
                "message": f"Berhasil memproses fill_flag{total_processed} CNOTEs" +
                           (f", gagal {total_failed} CNOTEs" if total_failed > 0 else ""),
                "processed": total_processed,
                "failed": total_failed,
                "batches_processed": len(batch_groups)
            }

        except Exception as e:
            connection.rollback()
            logger.error(f"Gagal memproses fill_flag CNOTE: {str(e)}")
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

