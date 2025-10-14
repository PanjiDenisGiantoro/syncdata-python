from celery import Celery
from celery.schedules import crontab
import sentry_config
celery_app = Celery(
    'flight_schedule',
    broker='mongodb://localhost:27017/celery',
    backend='mongodb://localhost:27017/celery',
    include=['cron.celery_task']  # arahkan ke folder cron
)

celery_app.conf.beat_schedule = {
    # 'insert-flight-schedule': {
    #     'task': 'cron.celery_task.all_flight_schedule',  # full path
    #     'schedule': crontab(hour=7, minute=0)
    # },
    # 'insert-flight-big-iata': {
    #     'task': 'cron.celery_task.flight_big_iata',      # full path
    #     'schedule': crontab(hour=9, minute=20)
    # },
   'update_tco_tci_v2': {
        'task': 'cron.celery_task.proc_update_tco_tci_v2',
        'schedule': crontab(hour=13, minute=50)
    },
#      'sync_run_ctc_day': {
#          'task': 'cron.celery_task.proc_sync_run_ctc_day',
#          'schedule': crontab(hour=7, minute=30)
#      },

    #  'proc_update_rep_stg_charge_bag': {
    #      'task': 'cron.celery_task.proc_update_rep_stg_charge_bag',
    #      'schedule': crontab(hour=13, minute=2)
    #  },     
    #  'proc_btbpbd': {
    #      'task': 'cron.celery_task.proc_btbpbd',
    #      'schedule': crontab(hour=15, minute=10)
    #  },
     
     
#    'proc_sync_dci_141': {
#        'task': 'cron.celery_task.proc_sync_dci_141',
#        'schedule': crontab(hour=14, minute=12)
#    },

   
    #  'sync_ctc': {
    #      'task': 'cron.celery_task.sync_ctc',
    #      'schedule': crontab(hour=14, minute=12)
    #  }
    # 'sync_ctc': {
    #      'task': 'cron.celery_task.proc_sync_run_ctc_insert',
    #      'schedule': crontab(hour=15, minute=57)
    #  }
#    'proc_sync_dci': {
#        'task': 'cron.celery_task.proc_sync_dci',
#        'schedule': crontab(hour=10, minute=43)
#    }
}

celery_app.conf.timezone = 'Asia/Jakarta'
