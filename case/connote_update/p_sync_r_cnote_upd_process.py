import oracledb
from config import Config
from db import get_oracle_connection_dbrbn, get_oracle_connection_billing
from case.connote_update import p_sync_r_cnote_upd_process


def p_sync_r_cnote_upd_process(p_cnote):
    try:
        # Step 1: Get data from CMS_CNOTE where CNOTE_NO = P_CNOTE
        connection = get_oracle_connection_dbrbn()  # Using the DB connection to DBRBN
        if connection:
            cursor = connection.cursor()
            query = f"""
                SELECT * FROM CMS_CNOTE A WHERE CNOTE_NO = :p_cnote
            """
            cursor.execute(query, {"p_cnote": p_cnote})
            cnote_data = cursor.fetchone()  # Assuming that CNOTE_NO is unique
            cursor.close()

            if cnote_data:
                # print(f"Found CNOTE data: {cnote_data}")

                # Step 2: Sync the data using the MERGE statement with Billing database
                connection_billing = get_oracle_connection_billing()
                if connection_billing:
                    cursor_billing = connection_billing.cursor()
                    merge_query = f"""
                   BEGIN
                        MERGE INTO JNEBILL.R_CMS_CNOTE A
                        USING (
                            SELECT * 
                            FROM jne.R_CMS_CNOTE@DBRBN 
                            WHERE R_CNOTE_NO = :p_cnote
                        ) R
                        ON (A.R_CNOTE_NO = :p_cnote)
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
                    cursor_billing.execute(merge_query, {"p_cnote": p_cnote})
                    connection_billing.commit()  # Commit the transaction
                    # Only close connection if it was created inside this function
                    if connection_billing != connection:
                        cursor_billing.close()
                        connection_billing.close()
                    else:
                        cursor_billing.close()
                    return {"status": "success", "message": "CNOTE updated/inserted successfully."}
                else:
                    raise Exception("Unable to connect to Billing database.")
            else:
                raise Exception(f"CNOTE data not found for the given CNOTE_NO: {p_cnote}.")

        else:
            raise Exception(f"CNOTE_NO {p_cnote} not found in DBRBN.")

    except Exception as e:
        # print(f"Error in syncing CNOTE {p_cnote}: {e}")
        return {"status": "error", "message": str(e)}
