import oracledb
from config import Config
from case.connote_update import p_update_cnote_bill_flag
from db import get_oracle_connection_dbrbn, get_oracle_connection_billing

def p_update_cnote_bill_flag(p_cnote, connection=None):
    try:
        # Step 1: Check if there's exactly 1 record for the given CNOTE_NO in CMS_CNOTE
        if connection is None:
            connection = get_oracle_connection_dbrbn()  # Using the DB connection to DBRBN
        if connection:
            cursor = connection.cursor()
            count_query = f"""
                SELECT COUNT(*) 
                FROM CMS_CNOTE A 
                WHERE CNOTE_NO = :p_cnote
            """
            cursor.execute(count_query, {"p_cnote": p_cnote})
            count = cursor.fetchone()[0]  # Fetch the count result

            if count == 1:
                # print(f"Found {p_cnote}. with update.")

                # Step 2: Update the BILL_FLAG to 'Y' for the given CNOTE_NO in the CONNOTE_UPDATE table
                if connection is None:
                    connection_updatedbrbn = get_oracle_connection_billing()  # Using the DB connection to Billing
                else:
                    connection_updatedbrbn = connection
                if connection_updatedbrbn:
                    cursor_updatedbrnm = connection_updatedbrbn.cursor()
                    update_query = f"""
                    UPDATE REPJNE.CONNOTE_UPDATE
                    SET BILL_FLAG = 'Y'
                    WHERE CNOTE_NO = :p_cnote
                    """
                    cursor_updatedbrnm.execute(update_query, {"p_cnote": p_cnote})
                    connection_updatedbrbn.commit()  # Commit the transaction
                    # Only close connection if it was created inside this function
                    if connection_updatedbrbn != connection:
                        cursor_updatedbrnm.close()
                        connection_updatedbrbn.close()
                    else:
                        cursor_updatedbrnm.close()
                    return {"status": "success", "message": "BILL_FLAG updated successfully for CNOTE."}
                else:
                    raise Exception("Unable to connect to Billing database.")
            else:
                raise Exception(f"Expected 1 record for CNOTE_NO {p_cnote}, but found {count} records.")

            cursor.close()
            if connection is not None:
                connection.close()

        else:
            raise Exception(f"Unable to connect to DBRBN for CNOTE_NO {p_cnote}.")

    except Exception as e:
        print(f"Error in updating BILL_FLAG for CNOTE {p_cnote}: {e}")
        return {"status": "error", "message": str(e)}
