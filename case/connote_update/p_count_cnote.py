import oracledb
from config import Config
from db import get_oracle_connection_billing
from datetime import datetime

def p_count_cnote(p_cnote_list):
    today_date = datetime.now().strftime('%Y-%m-%d')

    if not p_cnote_list:
        return 0

    conn = get_oracle_connection_billing()
    cursor = conn.cursor()

    try:
        # Hitung jumlah CNOTE_NO dalam list
        query = f"""
            SELECT COUNT(*) 
            FROM CMS_CNOTE
            WHERE CNOTE_NO IN ({', '.join([':cnote_' + str(i) for i in range(len(p_cnote_list))])})
        """
        params = {f'cnote_{i}': cnote for i, cnote in enumerate(p_cnote_list)}
        cursor.execute(query, params)
        count = cursor.fetchone()[0]

        # Ambil nilai existing dari MONITORING_SYNC_CNOTE
        select_query = """
            SELECT TOTAL_CNOTE_UPDATE 
            FROM MONITORING_SYNC_CNOTE 
            WHERE PERIODE = :today_date
        """
        cursor.execute(select_query, {'today_date': today_date})
        result = cursor.fetchone()
        previous_count = result[0] if result else 0

        # Tambahkan count baru ke count sebelumnya
        new_total = previous_count + count

        # Update nilai di MONITORING_SYNC_CNOTE
        update_query = """
            UPDATE MONITORING_SYNC_CNOTE 
            SET TOTAL_CNOTE_UPDATE = :new_total
            WHERE PERIODE = :today_date
        """
        cursor.execute(update_query, {
            'new_total': new_total,
            'today_date': today_date
        })

        # Commit perubahan
        conn.commit()
        return count

    except Exception as e:
        conn.rollback()
        raise e

    finally:
        cursor.close()
        conn.close()
