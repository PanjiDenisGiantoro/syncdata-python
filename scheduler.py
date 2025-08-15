import schedule
import time
import oracledb
import uuid
import requests
import json
import os
import time

from datetime import datetime
from db import get_oracle_connection_billing
from controller import get_flight
from typing import List, Dict, Any, Optional
from logger_config import logger
from itertools import islice

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

def get_active_iata_code():
    try:
        with connection.cursor() as cursor:
            # query = "SELECT DISTINCT IATA_CODE FROM MST_FLIGHT_CODE WHERE ACTIVE = 'Y'"
            query = "SELECT DISTINCT(IATA_CODE) FROM MST_FLIGHT_CODE WHERE IATA_CODE IS NOT NULL"
            cursor.execute(query)
            rows = cursor.fetchall()

            iata_codes = [row[0] for row in rows]
            return iata_codes
    except Exception as e:
        logger.error(f"Error fetching IATA codes: {str(e)}")
        return []

def chunked(iterable, size):
    it = iter(iterable)
    return iter(lambda: list(islice(it, size)), [])

def get_flight_data_today():
    """
    Fetches flight data from AviationStack API and processes it.
    Rotates API keys for each IATA code.
    Retries on 429 (Too Many Requests) with next key.
    """
    logger.info("Starting to fetch flight data...")

    iata_codes = get_active_iata_code()
    if not iata_codes:
        logger.warning("No active IATA codes found in the database.")
        return {"status": "completed", "message": "No flight data available", "processed": 0}

    access_keys = [os.getenv(f'ACCOUNT{i}') for i in range(1, 16)]
    access_keys = [key for key in access_keys if key]
    if not access_keys:
        logger.warning("No access keys found in the environment variables.")
        return {"status": "completed", "message": "No flight data available", "processed": 0}

    base_url = "https://api.aviationstack.com/v1/timetable"
    total_processed = 0
    details = []
    key_index = 0  # mulai dari API key pertama

    for iata_code in iata_codes:
        retries = 0
        while retries < len(access_keys):  # maksimal nyoba sebanyak jumlah API key
            access_key = access_keys[key_index]
            params = {
                'iataCode': iata_code,
                'type': 'departure',
                'access_key': access_key
            }

            try:
                logger.info(f"Fetching data for IATA code {iata_code} using access key {access_key}...")
                response = requests.get(base_url, params=params)
                
                if response.status_code == 429:
                    logger.warning(f"429 Too Many Requests for key {access_key}. Switching key...")
                    key_index = (key_index + 1) % len(access_keys)
                    retries += 1
                    time.sleep(10)  # delay saat ganti API key
                    continue  # coba lagi dengan key baru
                
                response.raise_for_status()
                data = response.json()

                if not data.get('data'):
                    logger.warning(f"No data for {iata_code}")
                    break

                countData = data['pagination']['total']
                logger.info(f"Fetched {countData} data, now modifying table with iataCode => {iata_code}")
                result = updateOrInsert(data['data'])
                processed_count = result.get('processed', 0)
                total_processed += processed_count
                details.append({iata_code: processed_count})

                logger.info(f"Data processed for {iata_code}. Waiting 60 seconds before next code...")
                time.sleep(60)  # delay tiap ganti IATA code
                key_index = (key_index + 1) % len(access_keys)  # ganti key untuk code berikutnya
                break

            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching data for {iata_code} with key {access_key}: {str(e)}")
                key_index = (key_index + 1) % len(access_keys)
                retries += 1
                time.sleep(60)  # delay sebelum coba key lain

            except Exception as e:
                logger.error(f"Unexpected error for {iata_code}: {str(e)}")
                break

    # logger for completion
    logger.info("Completed fetching and processing flight schedules for each IATA code.")
    return {
        "status": "completed",
        "message": "Flight data processed",
        "processed": total_processed,
        "details": details
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
        FROM FLIGHT_SCHEDULE
        WHERE flight_id_origin_iata = :flight_id_origin_iata
        AND flight_id_origin_icao = :flight_id_origin_icao
        AND departure_iata = :departure_iata
        AND arrival_iata = :arrival_iata
        AND schedule_departure = :schedule_departure
    """

    update_sql = """
        UPDATE FLIGHT_SCHEDULE
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
        INSERT INTO FLIGHT_SCHEDULE (
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
            # get iataCode from array 0 of flight_data and on departure then iataCode
            iataCode = flight_data[0]['departure']['iataCode']
            
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
    schedule.every().day.at("09:29").do(insertFlightLog) #17:00 PST = 08:00 WIB

    while True:
        schedule.run_pending()
        time.sleep(1)

