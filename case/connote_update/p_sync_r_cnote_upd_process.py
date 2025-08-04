import oracledb
from config import Config
from db import get_oracle_connection_dbrbn, get_oracle_connection_billing
from logger_config import logger


def process_cnote_batch(cursor, cnotes_batch):
    """Process a single batch of CNOTEs (max 1000 items)"""
    if not cnotes_batch:
        return 0, 0

    # Create parameters for the query
    bind_vars = {f"cnote_{i}": cnote for i, cnote in enumerate(cnotes_batch)}
    in_clause = ", ".join(f":cnote_{i}" for i in range(len(cnotes_batch)))

    merge_query = f"""
    BEGIN
            MERGE INTO JNEBILL.R_CMS_CNOTE A
            USING (
                SELECT *
                FROM jne.R_CMS_CNOTE@DBRBN
                WHERE R_CNOTE_NO IN ({in_clause})
            ) R
            ON (A.R_CNOTE_NO = R.R_CNOTE_NO)
            WHEN NOT MATCHED THEN
                INSERT (R_CNOTE_NO, R_CNOTE_DATE, R_CNOTE_BRANCH_ID, R_CNOTE_CUST_NO,
                        R_CNOTE_ORIGIN, R_CNOTE_DESTINATION, R_CNOTE_ROUTE_CODE, R_CNOTE_SERVICES_CODE,
                        R_CNOTE_QTY, R_CNOTE_WEIGHT, R_CNOTE_PAYMENT_TYPE, R_CNOTE_GOODS_VALUE,
                        R_CNOTE_INSURANCE_ADM, R_CNOTE_INSURANCE_FEE, R_CNOTE_PACKING_FEE,
                        R_CNOTE_VAT_PPH, R_CNOTE_VAT_PPN, R_CNOTE_DISCOUNT, R_CNOTE_FREIGHT_CHARGE,
                        R_CNOTE_AMOUNT, R_CNOTE_AMOUNT_PAYMENT, R_CREATE_DATE,
                        R_CNOTE_SURCHARGE, R_CNOTE_AMOUNT_BEFORE_PPN)
                VALUES (R.R_CNOTE_NO, R.R_CNOTE_DATE, R.R_CNOTE_BRANCH_ID, R.R_CNOTE_CUST_NO,
                        R.R_CNOTE_ORIGIN, R.R_CNOTE_DESTINATION, R.R_CNOTE_ROUTE_CODE,
                        R.R_CNOTE_SERVICES_CODE, R.R_CNOTE_QTY, R.R_CNOTE_WEIGHT,
                        R.R_CNOTE_PAYMENT_TYPE, R.R_CNOTE_GOODS_VALUE, R.R_CNOTE_INSURANCE_ADM,
                        R.R_CNOTE_INSURANCE_FEE, R.R_CNOTE_PACKING_FEE, R.R_CNOTE_VAT_PPH,
                        R.R_CNOTE_VAT_PPN, R.R_CNOTE_DISCOUNT, R.R_CNOTE_FREIGHT_CHARGE,
                        R.R_CNOTE_AMOUNT, R.R_CNOTE_AMOUNT_PAYMENT, R.R_CREATE_DATE,
                        R.R_CNOTE_SURCHARGE, R.R_CNOTE_AMOUNT_BEFORE_PPN)
            WHEN MATCHED THEN
                UPDATE SET
                    R_CNOTE_DATE = R.R_CNOTE_DATE,
                    R_CNOTE_BRANCH_ID = R.R_CNOTE_BRANCH_ID,
                    R_CNOTE_CUST_NO = R.R_CNOTE_CUST_NO,
                    R_CNOTE_ORIGIN = R.R_CNOTE_ORIGIN,
                    R_CNOTE_DESTINATION = R.R_CNOTE_DESTINATION,
                    R_CNOTE_ROUTE_CODE = R.R_CNOTE_ROUTE_CODE,
                    R_CNOTE_SERVICES_CODE = R.R_CNOTE_SERVICES_CODE,
                    R_CNOTE_QTY = R.R_CNOTE_QTY,
                    R_CNOTE_WEIGHT = R.R_CNOTE_WEIGHT,
                    R_CNOTE_PAYMENT_TYPE = R.R_CNOTE_PAYMENT_TYPE,
                    R_CNOTE_GOODS_VALUE = R.R_CNOTE_GOODS_VALUE,
                    R_CNOTE_INSURANCE_ADM = R.R_CNOTE_INSURANCE_ADM,
                    R_CNOTE_INSURANCE_FEE = R.R_CNOTE_INSURANCE_FEE,
                    R_CNOTE_PACKING_FEE = R.R_CNOTE_PACKING_FEE,
                    R_CNOTE_VAT_PPH = R.R_CNOTE_VAT_PPH,
                    R_CNOTE_VAT_PPN = R.R_CNOTE_VAT_PPN,
                    R_CNOTE_DISCOUNT = R.R_CNOTE_DISCOUNT,
                    R_CNOTE_FREIGHT_CHARGE = R.R_CNOTE_FREIGHT_CHARGE,
                    R_CNOTE_AMOUNT = R.R_CNOTE_AMOUNT,
                    R_CNOTE_AMOUNT_PAYMENT = R.R_CNOTE_AMOUNT_PAYMENT,
                    R_CREATE_DATE = R.R_CREATE_DATE,
                    R_CNOTE_SURCHARGE = R.R_CNOTE_SURCHARGE,
                    R_CNOTE_AMOUNT_BEFORE_PPN = R.R_CNOTE_AMOUNT_BEFORE_PPN;
        END;
    """
    try:
        cursor.execute(merge_query, bind_vars)
        return len(cnotes_batch), 0
    except Exception as e:
        logger.error(f"Error processing batch of {len(cnotes_batch)} CNOTEs: {str(e)}")
        return 0, len(cnotes_batch)


def p_sync_r_cnote_upd_process(*batch_groups):
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
                "message": f"Berhasil memproses p_sync_r_cnote_upd_process {total_processed} CNOTEs" +
                           (f", gagal {total_failed} CNOTEs" if total_failed > 0 else ""),
                "processed": total_processed,
                "failed": total_failed,
                "batches_processed": len(batch_groups)
            }

            # Commit any remaining transactions
            connection.commit()

            return {
                "status": "success" if total_failed == 0 else "partial",
                "message": f"Berhasil memproses p_sync_r_cnote_upd_process{total_processed} CNOTEs" +
                           (f", gagal {total_failed} CNOTEs" if total_failed > 0 else ""),
                "processed": total_processed,
                "failed": total_failed,
                "batches_processed": len(batch_groups)
            }

        except Exception as e:
            connection.rollback()
            logger.error(f"Gagal memproses p_sync_r_cnote_upd_process CNOTE: {str(e)}")
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
