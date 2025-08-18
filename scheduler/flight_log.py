from datetime import datetime
from db import get_oracle_connection_billing
from logger_config import logger
from .flight_fetch_aviation import get_flight_data_today

connection = get_oracle_connection_billing()
def insertFlightLog():
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
            SELECT MIN(CREATED_AT)
            FROM FLIGHT_SCHEDULE
            WHERE TRUNC(CREATED_AT) = TRUNC(SYSDATE - 7)
            """)
            target_date = cursor.fetchone()[0]

            if not target_date:
                print(f"[{datetime.now()}] Tidak ada data untuk hari ke-7. Mengambil data hari ini...")
                get_flight_data_today()
                return

            cursor.execute("""
                SELECT *
                FROM FLIGHT_SCHEDULE
                WHERE TRUNC(created_at) = TRUNC(:created_at)
            """, {"created_at": target_date})
            rows = cursor.fetchall()

            print(f"[{datetime.now()}] Insert {len(rows)} record ke FLIGHT_SCHEDULE_LOG")

            if rows:
                logger.info(f"Insert {len(rows)} record ke FLIGHT_SCHEDULE_LOG")
                cursor.executemany("""
                    INSERT INTO FLIGHT_SCHEDULE_LOG (
                        ID, FLIGHT_ID_ORIGIN_IATA, FLIGHT_ID_ORIGIN_ICAO,
                        DEPARTURE_IATA, ARRIVAL_IATA,
                        SCHEDULE_DEPARTURE, ESTIMATE_RUNWAY_DEPARTURE,
                        SCHEDULE_ARRIVAL, ESTIMATE_RUNWAY_ARRIVAL,
                        AIRLINE, STATUS,
                        CREATED_AT, UPDATED_AT, RAWDATA
                    )
                    VALUES (
                        :ID, :FLIGHT_ID_ORIGIN_IATA, :FLIGHT_ID_ORIGIN_ICAO,
                        :DEPARTURE_IATA, :ARRIVAL_IATA,
                        :SCHEDULE_DEPARTURE, :ESTIMATE_RUNWAY_DEPARTURE,
                        :SCHEDULE_ARRIVAL, :ESTIMATE_RUNWAY_ARRIVAL,
                        :AIRLINE, :STATUS,
                        :CREATED_AT, :UPDATED_AT, :RAWDATA
                    )
                """, rows)
                connection.commit()

                cursor.execute("""
                    DELETE FROM FLIGHT_SCHEDULE
                    WHERE TRUNC(CREATED_AT) = TRUNC(:created_at)
                """, {"created_at": target_date})
                connection.commit()
                print(f"[{datetime.now()}] Data tanggal {target_date} dihapus dari DEV")
                get_flight_data_today()
            return

    except Exception as e:
        print("Error insert:", e)