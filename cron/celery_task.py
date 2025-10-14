from celery_app import celery_app
from schedulers.scheduler_flight.flight_log import insertFlightLog
from schedulers.scheduler_flight.flight_fetch_airlabs import insertFlightBigIata
import sentry_sdk
from schedulers.scheduler_sync_ctc.insert_tco_tci_v2 import sync_run_ctc
from schedulers.scheduler_sync_ctc.insert_tco_tci_v2 import sync_run_ctc_day

from schedulers.scheduler_sync_ctc.insert_tco_tci_v2 import sync_run_ctc_insert

from schedulers.scheduler_sync_ctc.run_ctc_procedures import update_btbpbd
from schedulers.scheduler_sync_ctc.run_ctc_procedures import update_tco_tci_v2

from schedulers.scheduler_sync_ctc.run_ctc_procedures import update_rep_stg_charge_bag

from schedulers.scheduler_sync_ctc.sync_dci import sync_dci
from schedulers.scheduler_sync_ctc.sync_dci_141 import sync_dci_141

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



@celery_app.task(name="cron.celery_task.sync_ctc")
def sync_ctc():
    try:
        sync_run_ctc()
    except Exception as e:
        sentry_sdk.capture_exception(e)
        raise


@celery_app.task(name="cron.celery_task.proc_sync_run_ctc_insert")
def proc_sync_run_ctc_insert():
    try:
        sync_run_ctc_insert()
    except Exception as e:
        sentry_sdk.capture_exception(e)
        raise

@celery_app.task(name="cron.celery_task.proc_btbpbd")
def proc_btbpbd():
    try:
        update_btbpbd()
    except Exception as e:
        sentry_sdk.capture_exception(e)
        raise


@celery_app.task(name="cron.celery_task.proc_update_tco_tci_v2")
def proc_update_tco_tci_v2():
    try:
        update_tco_tci_v2()
    except Exception as e:
        sentry_sdk.capture_exception(e)
        raise



@celery_app.task(name="cron.celery_task.proc_update_rep_stg_charge_bag")
def proc_update_rep_stg_charge_bag():
    try:
        update_rep_stg_charge_bag()
    except Exception as e:
        sentry_sdk.capture_exception(e)
        raise

@celery_app.task(name="cron.celery_task.proc_sync_run_ctc_day")
def proc_sync_run_ctc_day():
    try:
        sync_run_ctc_day()
    except Exception as e:
        sentry_sdk.capture_exception(e)
        raise


@celery_app.task(name="cron.celery_task.proc_sync_dci")
def proc_sync_dci():
    try:
        sync_dci()
    except Exception as e:
        sentry_sdk.capture_exception(e)
        raise

@celery_app.task(name="cron.celery_task.proc_sync_dci_141")
def proc_sync_dci_141():
    try:
        sync_dci_141()
    except Exception as e:
        sentry_sdk.capture_exception(e)
        raise