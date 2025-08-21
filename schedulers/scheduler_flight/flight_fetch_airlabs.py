import os, time, requests
from logger_schedule_config import logger
from .flight_repository import get_big_iata_code
from .flight_utils import updateOrInsert

def insertFlightBigIata():
    big_iata_codes = get_big_iata_code()
    if not big_iata_codes:
        logger.warning("No active IATA codes found in the database.")
        return

    access_keys = [os.getenv(f'ACCOUNTAIRLABS{i}') for i in range(1, 5)]
    access_keys = [key for key in access_keys if key]
    if not access_keys:
        logger.warning("No access keys found in env vars.")
        return

    base_url = "https://airlabs.co/api/v9/schedules"
    key_index = 0

    for iata_code in big_iata_codes:
        retries = 0
        while retries < len(access_keys):
            access_key = access_keys[key_index]
            params = {'dep_iata': iata_code, 'api_key': access_key}

            try:
                logger.info(f"Fetching Airlabs API for {iata_code} with key {access_key}")
                response = requests.get(base_url, params=params)
                response.raise_for_status()
                data = response.json()

                if not data.get('response'):
                    logger.warning(f"No data for {iata_code}")
                    break

                countData = data['request']['total_items']
                logger.info(f"Fetched {countData} data for IATA {iata_code}")
                updateOrInsert(data['response'])
                
                logger.info(f"{iata_code} done. Waiting 60 seconds before next code...")
                time.sleep(60)
                key_index = (key_index + 1) % len(access_keys)
                break

            except requests.exceptions.RequestException as e:
                logger.error(f"Error {iata_code} with key {access_key}: {str(e)}")
                key_index = (key_index + 1) % len(access_keys)
                retries += 1
                time.sleep(60)

            except Exception as e:
                logger.error(f"Unexpected error Airlabs API: {str(e)}")
                return

    logger.info("Completed fetching and processing flight schedules (Airlabs).")
