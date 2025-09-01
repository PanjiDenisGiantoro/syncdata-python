from celery import Celery
from celery.schedules import crontab
import sentry_config
celery_app = Celery(
    'flight_schedule',
    broker='mongodb://10.18.9.237:27017/celery',
    backend='mongodb://10.18.9.237:27017/celery',
    include=['cron.celery_task']  # arahkan ke folder cron
)

celery_app.conf.beat_schedule = {
    'insert-flight-schedule': {
        'task': 'cron.celery_task.all_flight_schedule',  # full path
        'schedule': crontab(hour=7, minute=0)
    },
    'insert-flight-big-iata': {
        'task': 'cron.celery_task.flight_big_iata',      # full path
        'schedule': crontab(hour=15, minute=20)
    },
    'sync_ctc':{
        'task': 'cron.celery_task.sync_ctc',
        'schedule': crontab(hour=16, minute=0)
    },
    'update_tco_tci_v2': {
        'task': 'cron.celery_task.update_tco_tci_v2',
        'schedule': crontab(hour=11, minute=15)
    }
}

celery_app.conf.timezone = 'Asia/Jakarta'
