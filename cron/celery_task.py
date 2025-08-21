from celery_app import celery_app
from schedulers.scheduler_flight.flight_log import insertFlightLog
from schedulers.scheduler_flight.flight_fetch_airlabs import insertFlightBigIata

@celery_app.task(name="cron.celery_task.all_flight_schedule")
def all_flight_schedule():
    insertFlightLog()

@celery_app.task(name="cron.celery_task.flight_big_iata")
def flight_big_iata():
    insertFlightBigIata()