import oracledb
from config import Config
from db import get_oracle_connection_billing
from logger_config import logger
from typing import List, Dict, Any, Optional
from datetime import datetime

# Konversi ISO 8601 string ke datetime object
def convert_iso_to_dt(dt_str: Optional[str]) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.split('.')[0])  # hapus .000 jika ada
    except ValueError as e:
        logger.warning(f"Invalid ISO format: {dt_str} -> {e}")
        return None

def p_sync_flight(flight_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not flight_data:
        logger.warning("No flight data provided for processing")
        return {"processed": 0, "message": "No flight data provided"}

    processed_count = 0

    try:
        conn = get_oracle_connection_billing()
        cursor = conn.cursor()

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
                updated_at
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
                :updated_at
            )
        """

        for flight in flight_data:
            try:
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
                    "updated_at": datetime.now()
                }

                logger.info(f"Inserting flight record: {flight_info}")
                cursor.execute(insert_sql, flight_info)
                processed_count += 1

                if processed_count % 10 == 0:
                    logger.info(f"Committed {processed_count} records so far...")
                    conn.commit()

            except Exception as e:
                logger.error(f"Error inserting record: {str(e)}")

        conn.commit()
        cursor.close()
        conn.close()

        return {
            "status": "success",
            "processed": processed_count,
            "message": f"Successfully inserted {processed_count} flight records"
        }

    except Exception as e:
        logger.error(f"Error processing flight data: {str(e)}")
        return {
            "status": "error",
            "processed": processed_count,
            "message": f"Error processing flight data: {str(e)}"
        }
