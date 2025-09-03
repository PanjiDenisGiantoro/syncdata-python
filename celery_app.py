# celery_app.py
from celery import Celery
from celery.schedules import crontab
import sentry_sdk
from celery_config import get_celery_schedule, init_celery_tasks

init_celery_tasks()

app = Celery(
    'flight_schedule',
    broker='mongodb://10.18.9.237:27017/celery',
    backend='mongodb://10.18.9.237:27017/celery',
    include=[
        'cron.celery_task',
        'schedulers.scheduler_backup.backup_utils'
    ]
)

# Set timezone
app.conf.timezone = 'Asia/Jakarta'

def update_celery_schedule():
    schedule = get_celery_schedule()
    for task_id, task_config in schedule.items():
        # Konversi ke format crontab
        schedule[task_id]['schedule'] = crontab(**task_config['schedule'])
    app.conf.beat_schedule = schedule

# Muat jadwal saat startup
update_celery_schedule()

@app.task
def reload_celery_schedule():
    update_celery_schedule()
    return "Jadwal berhasil dimuat ulang"