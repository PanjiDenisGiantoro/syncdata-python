import oracledb
from config import Config
from db import get_oracle_connection_dbrbn, get_oracle_connection_billing
import json
from datetime import datetime
from logger_config import logger


def process_cnote_batch(cursor, cnotes_batch):
    """Process a single batch of CNOTEs (max 1000 items)"""
    if not cnotes_batch:
        return 0, 0
        
    # Create parameters for the query
    bind_vars = {f"cnote_{i}": cnote for i, cnote in enumerate(cnotes_batch)}
    in_clause = ", ".join(f":cnote_{i}" for i in range(len(cnotes_batch)))

    merge_query = f"""
                                            MERGE INTO CMS_CNOTE B USING (
                                                SELECT a.*
                                                FROM CMS_CNOTE@DBRBN A
                                                WHERE CNOTE_NO IN ({in_clause})
                                            ) X
                                            ON (B.CNOTE_NO = X.CNOTE_NO)
                                            WHEN NOT MATCHED THEN
                                                INSERT (
                                                    CNOTE_NO, CNOTE_DATE, CNOTE_BRANCH_ID, CNOTE_AE_ID, CNOTE_PICKUP_NO, 
                                                    CNOTE_SERVICES_CODE, CNOTE_POD_DATE, CNOTE_POD_RECEIVER, CNOTE_POD_CODE, 
                                                    CNOTE_CUST_NO, CNOTE_ROUTE_CODE, CNOTE_ORIGIN, CNOTE_DESTINATION, 
                                                    CNOTE_QTY, CNOTE_WEIGHT, CNOTE_DIM, CNOTE_SHIPPER_NAME, CNOTE_SHIPPER_ADDR1, 
                                                    CNOTE_SHIPPER_ADDR2, CNOTE_SHIPPER_ADDR3, CNOTE_SHIPPER_CITY, CNOTE_SHIPPER_ZIP, 
                                                    CNOTE_SHIPPER_REGION, CNOTE_SHIPPER_COUNTRY, CNOTE_SHIPPER_CONTACT, CNOTE_SHIPPER_PHONE, 
                                                    CNOTE_RECEIVER_NAME, CNOTE_RECEIVER_ADDR1, CNOTE_RECEIVER_ADDR2, CNOTE_RECEIVER_ADDR3, 
                                                    CNOTE_RECEIVER_CITY, CNOTE_RECEIVER_ZIP, CNOTE_RECEIVER_REGION, CNOTE_RECEIVER_COUNTRY, 
                                                    CNOTE_RECEIVER_CONTACT, CNOTE_RECEIVER_PHONE, CNOTE_DELIVERY_NAME, CNOTE_DELIVERY_ADDR1, 
                                                    CNOTE_DELIVERY_ADDR2, CNOTE_DELIVERY_ADDR3, CNOTE_DELIVERY_CITY, CNOTE_DELIVERY_ZIP, 
                                                    CNOTE_DELIVERY_REGION, CNOTE_DELIVERY_COUNTRY, CNOTE_DELIVERY_CONTACT, CNOTE_DELIVERY_PHONE, 
                                                    CNOTE_DELIVERY_TYPE, CNOTE_GOODS_TYPE, CNOTE_GOODS_DESCR, CNOTE_GOODS_VALUE, 
                                                    CNOTE_SPECIAL_INS, CNOTE_INSURANCE_ID, CNOTE_INSURANCE_VALUE, CNOTE_PAYMENT_TYPE, 
                                                    CNOTE_CURRENCY, CNOTE_AMOUNT, CNOTE_ADDITIONAL_FEE, CNOTE_NOTICE, CNOTE_COMMISION, 
                                                    CNOTE_PRINTED, CNOTE_INVOICED, CNOTE_CANCEL, CNOTE_HOLD, CNOTE_HOLD_REASON, 
                                                    CNOTE_USER, CNOTE_DELIVERED, CNOTE_INBOUND, CNOTE_HOLDIT, CNOTE_HANDLING, 
                                                    CNOTE_MGTFEE, CNOTE_QRC, CNOTE_QUICK, CNOTE_REFNO, CNOTE_VERIFIED, 
                                                    CNOTE_VDATE, CNOTE_VUSER, CNOTE_RDATE, CNOTE_RUSER, CNOTE_RECEIVED, 
                                                    CNOTE_LUID, CNOTE_LDATE, CNOTE_EDIT, CNOTE_BILL_STATUS, CNOTE_MANIFEST_NO, 
                                                    CNOTE_RUNSHEET_NO, CNOTE_DO, CNOTE_INSURANCE_NO, CNOTE_OTHER_FEE, CNOTE_CURC_PAYMENT, 
                                                    CNOTE_BANK, CNOTE_CURC_RATE, CNOTE_PAYMENT_BY, CNOTE_AMOUNT_PAYMENT, CNOTE_TRANS, 
                                                    CNOTE_EUSER, CNOTE_ACT_WEIGHT, CNOTE_BILNOTE, CNOTE_CRDATE, CNOTE_CTC, 
                                                    CNOTE_YES_CANCEL, CNOTE_SMS, CNOTE_ECNOTE, CNOTE_CARD_NO, CNOTE_CARD_AMOUNT, 
                                                    CNOTE_CARD_DISC, CNOTE_PACKING
                                                )
                                                VALUES(
                                                    X.CNOTE_NO, X.CNOTE_DATE, X.CNOTE_BRANCH_ID, X.CNOTE_AE_ID, X.CNOTE_PICKUP_NO, 
                                                    X.CNOTE_SERVICES_CODE, X.CNOTE_POD_DATE, X.CNOTE_POD_RECEIVER, X.CNOTE_POD_CODE, 
                                                    X.CNOTE_CUST_NO, X.CNOTE_ROUTE_CODE, X.CNOTE_ORIGIN, X.CNOTE_DESTINATION, 
                                                    X.CNOTE_QTY, X.CNOTE_WEIGHT, X.CNOTE_DIM, X.CNOTE_SHIPPER_NAME, X.CNOTE_SHIPPER_ADDR1, 
                                                    X.CNOTE_SHIPPER_ADDR2, X.CNOTE_SHIPPER_ADDR3, X.CNOTE_SHIPPER_CITY, X.CNOTE_SHIPPER_ZIP, 
                                                    X.CNOTE_SHIPPER_REGION, X.CNOTE_SHIPPER_COUNTRY, X.CNOTE_SHIPPER_CONTACT, X.CNOTE_SHIPPER_PHONE, 
                                                    X.CNOTE_RECEIVER_NAME, X.CNOTE_RECEIVER_ADDR1, X.CNOTE_RECEIVER_ADDR2, X.CNOTE_RECEIVER_ADDR3, 
                                                    X.CNOTE_RECEIVER_CITY, X.CNOTE_RECEIVER_ZIP, X.CNOTE_RECEIVER_REGION, X.CNOTE_RECEIVER_COUNTRY, 
                                                    X.CNOTE_RECEIVER_CONTACT, X.CNOTE_RECEIVER_PHONE, X.CNOTE_DELIVERY_NAME, X.CNOTE_DELIVERY_ADDR1, 
                                                    X.CNOTE_DELIVERY_ADDR2, X.CNOTE_DELIVERY_ADDR3, X.CNOTE_DELIVERY_CITY, X.CNOTE_DELIVERY_ZIP, 
                                                    X.CNOTE_DELIVERY_REGION, X.CNOTE_DELIVERY_COUNTRY, X.CNOTE_DELIVERY_CONTACT, X.CNOTE_DELIVERY_PHONE, 
                                                    X.CNOTE_DELIVERY_TYPE, X.CNOTE_GOODS_TYPE, X.CNOTE_GOODS_DESCR, X.CNOTE_GOODS_VALUE, 
                                                    X.CNOTE_SPECIAL_INS, X.CNOTE_INSURANCE_ID, X.CNOTE_INSURANCE_VALUE, X.CNOTE_PAYMENT_TYPE, 
                                                    X.CNOTE_CURRENCY, X.CNOTE_AMOUNT, X.CNOTE_ADDITIONAL_FEE, X.CNOTE_NOTICE, X.CNOTE_COMMISION, 
                                                    X.CNOTE_PRINTED, X.CNOTE_INVOICED, X.CNOTE_CANCEL, X.CNOTE_HOLD, X.CNOTE_HOLD_REASON, 
                                                    X.CNOTE_USER, X.CNOTE_DELIVERED, X.CNOTE_INBOUND, X.CNOTE_HOLDIT, X.CNOTE_HANDLING, 
                                                    X.CNOTE_MGTFEE, X.CNOTE_QRC, X.CNOTE_QUICK, X.CNOTE_REFNO, X.CNOTE_VERIFIED, 
                                                    X.CNOTE_VDATE, X.CNOTE_VUSER, X.CNOTE_RDATE, X.CNOTE_RUSER, X.CNOTE_RECEIVED, 
                                                    X.CNOTE_LUID, X.CNOTE_LDATE, X.CNOTE_EDIT, X.CNOTE_BILL_STATUS, X.CNOTE_MANIFEST_NO, 
                                                    X.CNOTE_RUNSHEET_NO, X.CNOTE_DO, X.CNOTE_INSURANCE_NO, X.CNOTE_OTHER_FEE, X.CNOTE_CURC_PAYMENT, 
                                                    X.CNOTE_BANK, X.CNOTE_CURC_RATE, X.CNOTE_PAYMENT_BY, X.CNOTE_AMOUNT_PAYMENT, X.CNOTE_TRANS, 
                                                    X.CNOTE_EUSER, X.CNOTE_ACT_WEIGHT, X.CNOTE_BILNOTE, X.CNOTE_CRDATE, X.CNOTE_CTC, 
                                                    X.CNOTE_YES_CANCEL, X.CNOTE_SMS, X.CNOTE_ECNOTE, X.CNOTE_CARD_NO, X.CNOTE_CARD_AMOUNT, 
                                                    X.CNOTE_CARD_DISC, X.CNOTE_PACKING
                                                )
                                            WHEN MATCHED THEN
                                                UPDATE SET
                                                CNOTE_DATE    =    X.CNOTE_DATE,
                                                CNOTE_BRANCH_ID    =    X.CNOTE_BRANCH_ID,
                                                CNOTE_AE_ID    =    X.CNOTE_AE_ID,
                                                CNOTE_PICKUP_NO    =    X.CNOTE_PICKUP_NO,
                                                CNOTE_SERVICES_CODE    =    X.CNOTE_SERVICES_CODE,
                                                CNOTE_POD_DATE    =    X.CNOTE_POD_DATE,
                                                CNOTE_POD_RECEIVER    =    X.CNOTE_POD_RECEIVER,
                                                CNOTE_POD_CODE    =    X.CNOTE_POD_CODE,
                                                CNOTE_CUST_NO    =    X.CNOTE_CUST_NO,
                                                CNOTE_ROUTE_CODE    =    X.CNOTE_ROUTE_CODE,
                                                CNOTE_ORIGIN    =    X.CNOTE_ORIGIN,
                                                CNOTE_DESTINATION    =    X.CNOTE_DESTINATION,
                                                CNOTE_QTY    =    X.CNOTE_QTY,
                                                CNOTE_WEIGHT    =    X.CNOTE_WEIGHT,
                                                CNOTE_DIM    =    X.CNOTE_DIM,
                                                CNOTE_SHIPPER_NAME    =    X.CNOTE_SHIPPER_NAME,
                                                CNOTE_SHIPPER_ADDR1    =    X.CNOTE_SHIPPER_ADDR1,
                                                CNOTE_SHIPPER_ADDR2    =    X.CNOTE_SHIPPER_ADDR2,
                                                CNOTE_SHIPPER_ADDR3    =    X.CNOTE_SHIPPER_ADDR3,
                                                CNOTE_SHIPPER_CITY    =    X.CNOTE_SHIPPER_CITY,
                                                CNOTE_SHIPPER_ZIP    =    X.CNOTE_SHIPPER_ZIP,
                                                CNOTE_SHIPPER_REGION    =    X.CNOTE_SHIPPER_REGION,
                                                CNOTE_SHIPPER_COUNTRY    =    X.CNOTE_SHIPPER_COUNTRY,
                                                CNOTE_SHIPPER_CONTACT    =    X.CNOTE_SHIPPER_CONTACT,
                                                CNOTE_SHIPPER_PHONE    =    X.CNOTE_SHIPPER_PHONE,
                                                CNOTE_RECEIVER_NAME    =    X.CNOTE_RECEIVER_NAME,
                                                CNOTE_RECEIVER_ADDR1    =    X.CNOTE_RECEIVER_ADDR1,
                                                CNOTE_RECEIVER_ADDR2    =    X.CNOTE_RECEIVER_ADDR2,
                                                CNOTE_RECEIVER_ADDR3    =    X.CNOTE_RECEIVER_ADDR3,
                                                CNOTE_RECEIVER_CITY    =    X.CNOTE_RECEIVER_CITY,
                                                CNOTE_RECEIVER_ZIP    =    X.CNOTE_RECEIVER_ZIP,
                                                CNOTE_RECEIVER_REGION    =    X.CNOTE_RECEIVER_REGION,
                                                CNOTE_RECEIVER_COUNTRY    =    X.CNOTE_RECEIVER_COUNTRY,
                                                CNOTE_RECEIVER_CONTACT    =    X.CNOTE_RECEIVER_CONTACT,
                                                CNOTE_RECEIVER_PHONE    =    X.CNOTE_RECEIVER_PHONE,
                                                CNOTE_DELIVERY_NAME    =    X.CNOTE_DELIVERY_NAME,
                                                CNOTE_DELIVERY_ADDR1    =    X.CNOTE_DELIVERY_ADDR1,
                                                CNOTE_DELIVERY_ADDR2    =    X.CNOTE_DELIVERY_ADDR2,
                                                CNOTE_DELIVERY_ADDR3    =    X.CNOTE_DELIVERY_ADDR3,
                                                CNOTE_DELIVERY_CITY    =    X.CNOTE_DELIVERY_CITY,
                                                CNOTE_DELIVERY_ZIP    =    X.CNOTE_DELIVERY_ZIP,
                                                CNOTE_DELIVERY_REGION    =    X.CNOTE_DELIVERY_REGION,
                                                CNOTE_DELIVERY_COUNTRY    =    X.CNOTE_DELIVERY_COUNTRY,
                                                CNOTE_DELIVERY_CONTACT    =    X.CNOTE_DELIVERY_CONTACT,
                                                CNOTE_DELIVERY_PHONE    =    X.CNOTE_DELIVERY_PHONE,
                                                CNOTE_DELIVERY_TYPE    =    X.CNOTE_DELIVERY_TYPE,
                                                CNOTE_GOODS_TYPE    =    X.CNOTE_GOODS_TYPE,
                                                CNOTE_GOODS_DESCR    =    X.CNOTE_GOODS_DESCR,
                                                CNOTE_GOODS_VALUE    =    X.CNOTE_GOODS_VALUE,
                                                CNOTE_SPECIAL_INS    =    X.CNOTE_SPECIAL_INS,
                                                CNOTE_INSURANCE_ID    =    X.CNOTE_INSURANCE_ID,
                                                CNOTE_INSURANCE_VALUE    =    X.CNOTE_INSURANCE_VALUE,
                                                CNOTE_PAYMENT_TYPE    =    X.CNOTE_PAYMENT_TYPE,
                                                CNOTE_CURRENCY    =    X.CNOTE_CURRENCY,
                                                CNOTE_AMOUNT    =    X.CNOTE_AMOUNT,
                                                CNOTE_ADDITIONAL_FEE    =    X.CNOTE_ADDITIONAL_FEE,
                                                CNOTE_NOTICE    =    X.CNOTE_NOTICE,
                            --                    CNOTE_INVOICED    =    X.CNOTE_INVOICED,
                                                CNOTE_CANCEL    =    X.CNOTE_CANCEL,
                                                CNOTE_HOLD    =    X.CNOTE_HOLD,
                                                CNOTE_HOLD_REASON    =    X.CNOTE_HOLD_REASON,
                                                CNOTE_USER    =    X.CNOTE_USER,
                                                CNOTE_DELIVERED    =    X.CNOTE_DELIVERED,
                                                CNOTE_INBOUND    =    X.CNOTE_INBOUND,
                                                CNOTE_HOLDIT    =    X.CNOTE_HOLDIT,
                                                CNOTE_HANDLING    =    X.CNOTE_HANDLING,
                                                CNOTE_MGTFEE    =    X.CNOTE_MGTFEE,
                                                CNOTE_QRC    =    X.CNOTE_QRC,
                                                CNOTE_QUICK    =    X.CNOTE_QUICK,
                                                CNOTE_REFNO    =    X.CNOTE_REFNO,
                            --                    CNOTE_VERIFIED    =    X.CNOTE_VERIFIED,
                            --                    CNOTE_VDATE    =    X.CNOTE_VDATE,
                            --                    CNOTE_VUSER    =    X.CNOTE_VUSER,
                                                CNOTE_RDATE    =    X.CNOTE_RDATE,
                                                CNOTE_RUSER    =    X.CNOTE_RUSER,
                                                CNOTE_RECEIVED    =    X.CNOTE_RECEIVED,
                                                CNOTE_LUID    =    X.CNOTE_LUID,
                                                CNOTE_LDATE    =    X.CNOTE_LDATE,
                                                CNOTE_EDIT    =    X.CNOTE_EDIT,
                                                CNOTE_BILL_STATUS    =    X.CNOTE_BILL_STATUS,
                                                CNOTE_MANIFEST_NO    =    X.CNOTE_MANIFEST_NO,
                                                CNOTE_RUNSHEET_NO    =    X.CNOTE_RUNSHEET_NO,
                                                CNOTE_DO    =    X.CNOTE_DO,
                                                CNOTE_INSURANCE_NO    =    X.CNOTE_INSURANCE_NO,
                                                CNOTE_OTHER_FEE    =    X.CNOTE_OTHER_FEE,
                                                CNOTE_TRANS    =    X.CNOTE_TRANS,
                                                CNOTE_CURC_PAYMENT    =    X.CNOTE_CURC_PAYMENT,
                                                CNOTE_BANK    =    X.CNOTE_BANK,
                                                CNOTE_CURC_RATE    =    X.CNOTE_CURC_RATE,
                                                CNOTE_PAYMENT_BY    =    X.CNOTE_PAYMENT_BY,
                                                CNOTE_AMOUNT_PAYMENT    =    X.CNOTE_AMOUNT_PAYMENT,
                                                CNOTE_ACT_WEIGHT    =    X.CNOTE_ACT_WEIGHT,
                                                CNOTE_EUSER    =    X.CNOTE_EUSER,
                                                CNOTE_BILNOTE    =    X.CNOTE_BILNOTE,
                                                CNOTE_CRDATE    =    X.CNOTE_CRDATE,
                                                CNOTE_CTC    =    X.CNOTE_CTC,
                                                CNOTE_YES_CANCEL    =    X.CNOTE_YES_CANCEL,
                                                CNOTE_SMS    =    X.CNOTE_SMS,
                                                CNOTE_ECNOTE    =    X.CNOTE_ECNOTE,
                                                CNOTE_CARD_NO    =    X.CNOTE_CARD_NO,
                                                CNOTE_CARD_AMOUNT    =    X.CNOTE_CARD_AMOUNT,
                                                CNOTE_CARD_DISC    =    X.CNOTE_CARD_DISC,
                                                CNOTE_PACKING    =    X.CNOTE_PACKING
                                            """
    try:
        cursor.execute(merge_query, bind_vars)
        return len(cnotes_batch), 0
    except Exception as e:
        logger.error(f"Error processing batch of {len(cnotes_batch)} CNOTEs: {str(e)}")
        return 0, len(cnotes_batch)

def p_sync_cnote_upd_process(*batch_groups):
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

