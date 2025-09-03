# celery_config.py
import json
import oracledb
from db import get_oracle_connection_billing
from logger_config import logger


def init_celery_tasks():
    """Inisialisasi tabel dan data default"""
    conn = None
    try:
        conn = get_oracle_connection_billing()
        cursor = conn.cursor()

        # Daftar task default
        default_tasks = [
            {
                'id': 'insert-flight-schedule',
                'task': 'cron.celery_task.all_flight_schedule',
                'schedule': '0 7 * * *',
                'enabled': 1,
                'args': '[]',
                'kwargs': '{}',
                'options': '{}'
            },
            {
                'id': 'insert-flight-big-iata',
                'task': 'cron.celery_task.flight_big_iata',
                'schedule': '20 15 * * *',
                'enabled': 1,
                'args': '[]',
                'kwargs': '{}',
                'options': '{}'
            },
            {
                'id': 'exampletest',
                'task': 'cron.celery_task.exampletest',
                'schedule': '38 15 * * *',
                'enabled': 1,
                'args': '[]',
                'kwargs': '{}',
                'options': '{}'
            }
        ]

        for task in default_tasks:
            cursor.execute("""
            MERGE INTO celery_tasks t
            USING (SELECT :id as id FROM dual) s
            ON (t.id = s.id)
            WHEN NOT MATCHED THEN
                INSERT (id, task, schedule, enabled, args, kwargs, options)
                VALUES (:id, :task, :schedule, :enabled, :args, :kwargs, :options)
            """, task)

        conn.commit()
        logger.info("Tabel celery_tasks berhasil diinisialisasi")

    except Exception as e:
        logger.error(f"Gagal menginisialisasi celery_tasks: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def get_celery_schedule():
    """Mengambil jadwal dari database"""
    conn = None
    try:
        conn = get_oracle_connection_billing()
        cursor = conn.cursor()

        cursor.execute("""
                       SELECT id, task, schedule, args, kwargs, options
                       FROM celery_tasks
                       WHERE enabled = 1
                       """)

        schedule = {}
        for row in cursor:
            task_id, task, cron_expr, args, kwargs, options = row
            try:
                # Parse cron expression (format: "minute hour day month day_of_week")
                minute, hour, day_of_month, month, day_of_week = cron_expr.split()

                schedule[task_id] = {
                    'task': task,
                    'schedule': {
                        'minute': minute,
                        'hour': hour,
                        'day_of_month': day_of_month,
                        'month_of_year': month,
                        'day_of_week': day_of_week
                    },
                    'args': json.loads(args),
                    'kwargs': json.loads(kwargs),
                    'options': json.loads(options)
                }
            except Exception as e:
                logger.error(f"Gagal memproses task {task_id}: {str(e)}")
                continue

        return schedule

    except Exception as e:
        logger.error(f"Gagal mengambil jadwal dari database: {str(e)}")
        return {}
    finally:
        if conn:
            conn.close()