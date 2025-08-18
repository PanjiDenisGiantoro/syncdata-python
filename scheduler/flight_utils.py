import json
from datetime import datetime
from typing import List, Dict, Any, Optional
from db import get_oracle_connection_billing
from logger_config import logger

connection = get_oracle_connection_billing()

def convert_iso_to_dt(dt_str: Optional[str]) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.split('.')[0])
    except ValueError as e:
        logger.warning(f"Invalid ISO format: {dt_str} -> {e}")
        return None

def updateOrInsert(flight_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not flight_data:
        logger.warning("No flight data provided")
        return {"processed": 0, "message": "No data"}

    processed_count = 0
    select_sql = """SELECT COUNT(1) FROM FLIGHT_SCHEDULE
                    WHERE flight_id_origin_iata = :flight_id_origin_iata
                    AND flight_id_origin_icao = :flight_id_origin_icao
                    AND departure_iata = :departure_iata
                    AND arrival_iata = :arrival_iata
                    AND schedule_departure = :schedule_departure"""

    update_sql = """UPDATE FLIGHT_SCHEDULE SET
                        estimate_runway_departure = :estimate_runway_departure,
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
                    AND schedule_departure = :schedule_departure"""

    insert_sql = """INSERT INTO FLIGHT_SCHEDULE (
                        flight_id_origin_iata, flight_id_origin_icao,
                        departure_iata, arrival_iata,
                        schedule_departure, estimate_runway_departure,
                        schedule_arrival, estimate_runway_arrival,
                        airline, status, created_at, updated_at, rawdata
                    ) VALUES (
                        :flight_id_origin_iata, :flight_id_origin_icao,
                        :departure_iata, :arrival_iata,
                        :schedule_departure, :estimate_runway_departure,
                        :schedule_arrival, :estimate_runway_arrival,
                        :airline, :status, :created_at, :updated_at, :rawdata
                    )"""

    try:
        with connection.cursor() as cursor:
            for flight in flight_data:
                is_airlabs = "flight_iata" in flight
                flight_info = {
                    "flight_id_origin_iata": flight.get("flight_iata") if is_airlabs else flight.get("flight", {}).get("iataNumber"),
                    "flight_id_origin_icao": flight.get("flight_icao") if is_airlabs else flight.get("flight", {}).get("icaoNumber"),
                    "departure_iata": flight.get("dep_iata") if is_airlabs else flight.get("departure", {}).get("iataCode"),
                    "arrival_iata": flight.get("arr_iata") if is_airlabs else flight.get("arrival", {}).get("iataCode"),
                    "schedule_departure": convert_iso_to_dt(flight.get("dep_time") if is_airlabs else flight.get("departure", {}).get("scheduledTime")),
                    "estimate_runway_departure": convert_iso_to_dt(flight.get("dep_estimated") if is_airlabs else flight.get("departure", {}).get("estimatedRunway")),
                    "schedule_arrival": convert_iso_to_dt(flight.get("arr_time") if is_airlabs else flight.get("arrival", {}).get("scheduledTime")),
                    "estimate_runway_arrival": convert_iso_to_dt(flight.get("arr_estimated") if is_airlabs else flight.get("arrival", {}).get("estimatedRunway")),
                    "airline": flight.get("airline_iata") if is_airlabs else flight.get("airline", {}).get("name"),
                    "status": flight.get("status"),
                    "created_at": datetime.now(),
                    "updated_at": datetime.now(),
                    "rawdata": json.dumps(flight)
                }

                cursor.execute(select_sql, {
                    "flight_id_origin_iata": flight_info["flight_id_origin_iata"],
                    "flight_id_origin_icao": flight_info["flight_id_origin_icao"],
                    "departure_iata": flight_info["departure_iata"],
                    "arrival_iata": flight_info["arrival_iata"],
                    "schedule_departure": flight_info["schedule_departure"],
                })
                exists = cursor.fetchone()[0] > 0

                if exists:
                    cursor.execute(update_sql, {**flight_info})
                else:
                    cursor.execute(insert_sql, flight_info)

                processed_count += 1

            connection.commit()
        return {"processed": processed_count, "message": f"{processed_count} processed"}
    except Exception as e:
        logger.error(f"Error in updateOrInsert: {str(e)}")
        return {"processed": processed_count, "message": f"Error: {str(e)}"}
