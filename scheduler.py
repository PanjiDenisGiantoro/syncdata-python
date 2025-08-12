import schedule
import time
import oracledb
import uuid
import requests
import json

from datetime import datetime
from db import get_oracle_connection_billing
from controller import get_flight
from typing import List, Dict, Any, Optional
from logger_config import logger

connection = get_oracle_connection_billing()
def insertFlightLog():
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
            SELECT MIN(CREATED_AT)
            FROM FLIGHT_SCHEDULE_DEV
            WHERE TRUNC(CREATED_AT) = TRUNC(SYSDATE - 7)
            """)
            target_date = cursor.fetchone()[0]

            if not target_date:
                print(f"[{datetime.now()}] Tidak ada data untuk hari ke-7.")
                get_flight_data_today()
                return

            cursor.execute("""
                SELECT *
                FROM FLIGHT_SCHEDULE_DEV
                WHERE TRUNC(created_at) = TRUNC(:created_at)
            """, {"created_at": target_date})
            rows = cursor.fetchall()

            print(f"[{datetime.now()}] Insert {len(rows)} record ke FLIGHT_SCHEDULE_LOG")

            if rows:
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
                    DELETE FROM FLIGHT_SCHEDULE_DEV
                    WHERE TRUNC(CREATED_AT) = TRUNC(:created_at)
                """, {"created_at": target_date})
                connection.commit()

                print(f"[{datetime.now()}] Data tanggal {target_date} dihapus dari DEV")

                job_id = str(uuid.uuid4())  # Generate unique job ID for the current request
                get_flight(job_id)
            return

    except Exception as e:
        print("Error insert:", e)



def get_flight_data_today():
    """
    Fetches flight data from AviationStack API and processes it.

    Args:
         (str): Unique identifier for the job

    Returns:
        dict: Processing results and status
    """
    logger.info(f"Job ID : Starting to fetch flight data...")

    # API Configuration
    base_url = "https://api.aviationstack.com/v1/timetable"
    params = {
        'iataCode': 'CGK',
        'type': 'departure',
        # 'access_key': '4a75614d656449337e99fac724ffc997'
        'access_key': 'b2e75f4982eed92a6502d1e6fa4f0984'
    }

    try:
        # Make API request
        logger.info(f"Job ID : Fetching data from AviationStack API...")
        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Raise exception for HTTP errors

        # Parse JSON response
        data = response.json()

        # Check if data was returned successfully
        if not data.get('data'):
            logger.warning(f"Job ID : No flight data found in API response")
            return {
                "status": "completed",
                "message": "No flight data available",
                "processed": 0
            }

        # Process the flight data
        logger.info(f"Job ID : Processing {len(data['data'])} flight records...")
        # result = p_sync_flight(data['data'])
        result = updateOrInsert(data['data'])

        logger.info(f"Job ID : Successfully processed flight data")
        return {
            "status": "completed",
            "message": "Flight data processed successfully",
            "processed": result.get('processed', 0),
            "details": result
        }

    except requests.exceptions.RequestException as e:
        error_msg = f"Error fetching flight data: {str(e)}"
        logger.error(f"Job ID : {error_msg}")
        return {
            "status": "error",
            "message": error_msg,
            "processed": 0
        }
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(f"Job ID : {error_msg}")
        return {
            "status": "error",
            "message": error_msg,
            "processed": 0
        }


def convert_iso_to_dt(dt_str: Optional[str]) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.split('.')[0])  # hapus .000 jika ada
    except ValueError as e:
        logger.warning(f"Invalid ISO format: {dt_str} -> {e}")
        return None


def updateOrInsert(flight_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not flight_data:
        logger.warning("No flight data provided for processing")
        return {"processed": 0, "message": "No flight data provided"}

    processed_count = 0

    select_sql = """
        SELECT COUNT(1)
        FROM flight_schedule_dev
        WHERE flight_id_origin_iata = :flight_id_origin_iata
          AND flight_id_origin_icao = :flight_id_origin_icao
          AND departure_iata = :departure_iata
          AND arrival_iata = :arrival_iata
          AND schedule_departure = :schedule_departure
    """

    update_sql = """
        UPDATE flight_schedule_dev
        SET estimate_runway_departure = :estimate_runway_departure,
            schedule_arrival = :schedule_arrival,
            estimate_runway_arrival = :estimate_runway_arrival,
            airline = :airline,
            status = :status,
            updated_at = :updated_at,
            rawdata = :rawdata
        WHERE flight_id_origin_iata = :flight_id_origin_iata
          AND flight_id_origin_icao = :flight_id_origin_icao
          AND departure_iata = :departure_iata
          AND arrival_iata = :arrival_iata
          AND schedule_departure = :schedule_departure
    """

    insert_sql = """
        INSERT INTO flight_schedule_dev (
            flight_id_origin_iata,
            flight_id_origin_icao,
            departure_iata,
            arrival_iata,
            schedule_departure,
            estimate_runway_departure,
            schedule_arrival,
            estimate_runway_arrival,
            airline,
            status,
            created_at,
            updated_at,
            rawdata
        ) VALUES (
            :flight_id_origin_iata,
            :flight_id_origin_icao,
            :departure_iata,
            :arrival_iata,
            :schedule_departure,
            :estimate_runway_departure,
            :schedule_arrival,
            :estimate_runway_arrival,
            :airline,
            :status,
            :created_at,
            :updated_at,
            :rawdata
        )
    """

    try:
        with connection.cursor() as cursor:
            for flight in flight_data:
                flight_info = {
                    "flight_id_origin_iata": flight.get("flight", {}).get("iataNumber"),
                    "flight_id_origin_icao": flight.get("flight", {}).get("icaoNumber"),
                    "departure_iata": flight.get("departure", {}).get("iataCode"),
                    "arrival_iata": flight.get("arrival", {}).get("iataCode"),
                    "schedule_departure": convert_iso_to_dt(flight.get("departure", {}).get("scheduledTime")),
                    "estimate_runway_departure": convert_iso_to_dt(flight.get("departure", {}).get("estimatedRunway")),
                    "schedule_arrival": convert_iso_to_dt(flight.get("arrival", {}).get("scheduledTime")),
                    "estimate_runway_arrival": convert_iso_to_dt(flight.get("arrival", {}).get("estimatedRunway")),
                    "airline": flight.get("airline", {}).get("name"),
                    "status": flight.get("status"),
                    "created_at": datetime.now(),
                    "updated_at": datetime.now(),
                    "rawdata": json.dumps(flight)
                }
                select_params = {
                    "flight_id_origin_iata": flight_info["flight_id_origin_iata"],
                    "flight_id_origin_icao": flight_info["flight_id_origin_icao"],
                    "departure_iata": flight_info["departure_iata"],
                    "arrival_iata": flight_info["arrival_iata"],
                    "schedule_departure": flight_info["schedule_departure"],
                }

                update_params = {
                    "flight_id_origin_iata": flight_info["flight_id_origin_iata"],
                    "flight_id_origin_icao": flight_info["flight_id_origin_icao"],
                    "departure_iata": flight_info["departure_iata"],
                    "arrival_iata": flight_info["arrival_iata"],
                    "schedule_departure": flight_info["schedule_departure"],
                    "estimate_runway_departure": flight_info["estimate_runway_departure"],
                    "schedule_arrival": flight_info["schedule_arrival"],
                    "estimate_runway_arrival": flight_info["estimate_runway_arrival"],
                    "airline": flight_info["airline"],
                    "status": flight_info["status"],
                    "updated_at": flight_info["updated_at"],
                    "rawdata": flight_info["rawdata"]
                }

                cursor.execute(select_sql, select_params)
                exists = cursor.fetchone()[0] > 0

                if exists:
                    print(f"Flight {flight_info['flight_id_origin_iata']} already exists, updating")
                    cursor.execute(update_sql, update_params)
                else:
                    cursor.execute(insert_sql, flight_info)

                processed_count += 1

            connection.commit()

        return {
            "processed": processed_count,
            "message": f"{processed_count} flight records processed (insert/update)"
        }

    except Exception as e:
        logger.error(f"Error in updateOrInsert: {str(e)}")
        return {
            "processed": processed_count,
            "message": f"Error: {str(e)}"
        }


def run_schedule_flight():
    schedule.every().day.at("17:00").do(insertFlightLog) #17:00 PST = 08:00 WIB

    while True:
        schedule.run_pending()
        time.sleep(1)

