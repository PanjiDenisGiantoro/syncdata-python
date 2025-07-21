import oracledb
from config import Config
from db import get_oracle_connection_billing, get_oracle_connection_dbrbn


def p_get_bag_no(bag_no, transit_manifest=None, p_date=None):
    print(f"DEBUG: bag_no={bag_no}, transit_manifest={transit_manifest}")  # TEST: tampilkan lemparan data
    try:
        connection = get_oracle_connection_dbrbn()
        if connection:
            cursor = connection.cursor()

            # 1. Ambil SMU dari function JNE.F_GET_DSMU_NO
            v_smu_no = None
            if bag_no and transit_manifest:
                cursor.execute("SELECT JNE.F_GET_DSMU_NO(:bag_no, :transit_manifest) FROM DUAL", {
                    'bag_no': bag_no,
                    'transit_manifest': transit_manifest
                })
                v_smu_no = cursor.fetchone()[0]

            if not v_smu_no:
                print(f"SKIP: F_GET_DSMU_NO result is NULL for bag_no={bag_no}, transit_manifest={transit_manifest}")
                return {"status": "skipped", "reason": "F_GET_DSMU_NO returned NULL"}

            # 2. Ambil detail MSMU
            cursor.execute('''
                SELECT 
                    A.MSMU_NO, 
                    A.MSMU_FLIGHT_NO, 
                    A.MSMU_STATUS, 
                    A.MSMU_MODA,
                    SUBSTR(A.MSMU_ORIGIN, 1, 3),
                    (SELECT MSMU_MODA_DESC FROM CMS_MSMU_MODA WHERE MSMU_MODA_ID = A.MSMU_MODA),
                    (SELECT MSMU_TYPE_DESC FROM CMS_MSMU_TYPE WHERE MSMU_TYPE_ID = A.MSMU_STATUS AND MSMU_MODA = A.MSMU_MODA)
                FROM CMS_MSMU A
                WHERE A.MSMU_NO = :v_smu_no
            ''', {'v_smu_no': v_smu_no})
            msmu_detail = cursor.fetchone()
            if not msmu_detail:
                print(f"SKIP: No MSMU detail found for SMU_NO={v_smu_no}")
                return {"status": "skipped", "reason": "No MSMU detail found"}

            # 3. Update CMS_COST_TRANSIT_V2
            update_query = '''
                UPDATE JNE.CMS_COST_TRANSIT_V2
                SET 
                    MODA = :v_moda,
                    MODA_DESC = :v_moda_desc,
                    MODA_TYPE = :v_moda_type,
                    MODA_TYPE_DESC = :v_moda_type_desc,
                    SMU_NUMBER = :v_smu_no,
                    FLIGHT_NUMBER = :v_smu_flight_no,
                    BRANCH_TRANSPORTER = :v_branch_transporter
                WHERE TRUNC(TRANSIT_MANIFEST_DATE) = :p_date
                  AND BAG_NO = :bag_no
                  AND TRANSIT_MANIFEST = :transit_manifest
            '''
            cursor.execute(update_query, {
                'v_moda': msmu_detail[3],
                'v_moda_desc': msmu_detail[5],
                'v_moda_type': msmu_detail[2],
                'v_moda_type_desc': msmu_detail[6],
                'v_smu_no': msmu_detail[0],
                'v_smu_flight_no': msmu_detail[1],
                'v_branch_transporter': msmu_detail[4],
                'p_date': p_date,
                'bag_no': bag_no,
                'transit_manifest': transit_manifest
            })
            connection.commit()
            print(f"UPDATED: CMS_COST_TRANSIT_V2 for bag_no={bag_no}, transit_manifest={transit_manifest}")
            return {"status": "success", "bag_no": bag_no, "transit_manifest": transit_manifest}

        else:
            raise Exception("Unable to connect to the database.")

    except Exception as e:
        print(f"Error in p_get_bag_no: {e}")
        return {"status": "error", "message": str(e)}

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()
