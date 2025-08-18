from db import get_oracle_connection_billing
from logger_config import logger

connection = get_oracle_connection_billing()

def get_active_iata_code():
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT DISTINCT(IATA_CODE) FROM MST_FLIGHT_CODE WHERE IATA_CODE IS NOT NULL")
            rows = cursor.fetchall()
            return [row[0] for row in rows]
    except Exception as e:
        logger.error(f"Error fetching active IATA codes: {str(e)}")
        return []

def get_big_iata_code():
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT departure_iata
                FROM FLIGHT_SCHEDULE
                GROUP BY departure_iata
                ORDER BY COUNT(*) DESC
                FETCH FIRST 15 ROWS ONLY
            """)
            rows = cursor.fetchall()
            return [row[0] for row in rows]
    except Exception as e:
        logger.error(f"Error fetching big IATA codes: {str(e)}")
        return []
