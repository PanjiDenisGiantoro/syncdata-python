import oracledb
from config import Config
from db import get_oracle_connection_dbrbn, get_oracle_connection_billing
from logger_config import logger


def p_sync_r_cnote_upd_process(p_cnote_list):
    if not p_cnote_list:
        return {"status": "error", "message": "Tidak ada nomor CNOTE yang diberikan"}

    connection = None
    try:
        connection = get_oracle_connection_billing()
        if not connection:
            return {"status": "error", "message": "Gagal terhubung ke database billing"}

        cursor = connection.cursor()

        # Prepare bind variables and IN clause
        bind_vars = {f"cnote_{i}": cnote for i, cnote in enumerate(p_cnote_list)}
        in_clause = ", ".join(f":cnote_{i}" for i in range(len(p_cnote_list)))

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

        cursor.execute(merge_query, bind_vars)
        connection.commit()

        return {
            "status": "success",
            "message": f"Berhasil memproses {len(p_cnote_list)} CNOTE",
            "processed": len(p_cnote_list)
        }

    except Exception as e:
        if connection:
            connection.rollback()
        logger.error(f"Gagal memproses p_r_sync_cnote_upd_process CNOTE: {str(e)}")
        return {
            "status": "error",
            "message": f"Gagal memproses CNOTE: {str(e)}",
            "failed": len(p_cnote_list)
        }
    finally:
        if connection:
            connection.close()


