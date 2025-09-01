from celery_app import celery_app
from schedulers.scheduler_flight.flight_log import insertFlightLog
from schedulers.scheduler_flight.flight_fetch_airlabs import insertFlightBigIata
import sentry_sdk
from schedulers.scheduler_sync_ctc.insert_tco_tci_v2 import sync_run_ctc
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

@celery_app.task(name="cron.celery_task.sync_ctc")
def sync_ctc():
    try:
        sync_run_ctc()
    except Exception as e:
        sentry_sdk.capture_exception(e)
        raise

@celery_app.task(name="cron.celery_task.update_tco_tci_v2")
def update_tco_tci_v2():
    try:
        update_tco_tci_v2()
    except Exception as e:
        sentry_sdk.capture_exception(e)
        raise

@celery_app.task(name="cron.celery_task.sync_run_ctc_day")
def sync_run_ctc_day():
    try:
        sync_run_ctc_day()
    except Exception as e:
        sentry_sdk.capture_exception(e)
        raise