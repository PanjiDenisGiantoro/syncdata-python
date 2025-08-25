from celery_app import celery_app
from schedulers.scheduler_flight.flight_log import insertFlightLog
from schedulers.scheduler_flight.flight_fetch_airlabs import insertFlightBigIata
import sentry_sdk
import requests

@celery_app.task(name="cron.celery_task.all_flight_schedule")
def all_flight_schedule():
    try:
        insertFlightLog()
    except Exception as e:
        sentry_sdk.capture_exception(e)
        raise  # penting agar Celery tetap tandai task gagal

@celery_app.task(name="cron.celery_task.flight_big_iata")
def flight_big_iata():
    try:
        insertFlightBigIata()
    except Exception as e:
        sentry_sdk.capture_exception(e)
        raise