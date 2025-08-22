import os, time, requests
from logger_config import logger
from .flight_repository import get_active_iata_code
from .flight_utils import updateOrInsert

def get_flight_data_today():
    logger.info("Starting to fetch AviationStack flight data...")

    iata_codes = get_active_iata_code()
    if not iata_codes:
        logger.warning("No active IATA codes found.")
        return

    access_keys = [os.getenv(f'ACCOUNT{i}') for i in range(1, 17)]
    access_keys = [key for key in access_keys if key]
    if not access_keys:
        logger.warning("No AviationStack keys found.")
        return

    base_url = "https://api.aviationstack.com/v1/timetable"
    key_index = 0

    for iata_code in iata_codes:
        retries = 0
        while retries < len(access_keys):
            access_key = access_keys[key_index]
            params = {'iataCode': iata_code, 'type': 'departure', 'access_key': access_key}

            try:
                logger.info(f"Fetching AviationStack for {iata_code} with key {access_key}")
                response = requests.get(base_url, params=params)

                if response.status_code == 429:
                    logger.warning(f"429 Too Many Requests for key {access_key}, switch key")
                    key_index = (key_index + 1) % len(access_keys)
                    retries += 1
                    time.sleep(10)
                    continue

                response.raise_for_status()
                data = response.json()

                if not data.get('data'):
                    logger.warning(f"No data for {iata_code}")
                    break

                countData = data['pagination']['total']
                logger.info(f"Fetched {countData} data for IATA {iata_code}")
                updateOrInsert(data['data'])

                logger.info(f"{iata_code} done. Waiting 60 seconds before next code...")
                time.sleep(60)
                key_index = (key_index + 1) % len(access_keys)
                break

            except requests.exceptions.RequestException as e:
                logger.error(f"Error AviationStack {iata_code} with key {access_key}: {str(e)}")
                key_index = (key_index + 1) % len(access_keys)
                retries += 1
                time.sleep(60)

            except Exception as e:
                logger.error(f"Unexpected error AviationStack {iata_code}: {str(e)}")
                break

    logger.info("Completed fetching and processing AviationStack flights.")
