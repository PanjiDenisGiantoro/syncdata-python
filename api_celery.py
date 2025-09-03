# api_celery.py
from flask import Blueprint, request, jsonify
from db import get_oracle_connection_billing
import json
from celery_app import app, update_celery_schedule
from logger_config import logger

bp = Blueprint('celery_api', __name__)


@bp.route('/api/celery/tasks', methods=['GET'])
def get_tasks():
    """Mendapatkan daftar semua task"""
    conn = None
    try:
        conn = get_oracle_connection_billing()
        cursor = conn.cursor()

        cursor.execute("""
                       SELECT id,
                              task,
                              schedule,
                              enabled,
                              args,
                              kwargs,
                              options,
                              TO_CHAR(last_updated, 'YYYY-MM-DD HH24:MI:SS') as last_updated
                       FROM celery_tasks
                       ORDER BY id
                       """)

        tasks = []
        for row in cursor:
            tasks.append({
                'id': row[0],
                'task': row[1],
                'schedule': row[2],
                'enabled': bool(row[3]),
                'args': json.loads(row[4]) if row[4] else [],
                'kwargs': json.loads(row[5]) if row[5] else {},
                'options': json.loads(row[6]) if row[6] else {},
                'last_updated': row[7]
            })

        return jsonify(tasks)

    except Exception as e:
        logger.error(f"Gagal mengambil daftar task: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500
    finally:
        if conn:
            conn.close()


@bp.route('/api/celery/tasks/<task_id>', methods=['PUT'])
def update_task(task_id):
    """Memperbarui task"""
    data = request.json
    conn = None

    try:
        conn = get_oracle_connection_billing()
        cursor = conn.cursor()

        cursor.execute("""
                       UPDATE celery_tasks
                       SET task         = :task,
                           schedule     = :schedule,
                           enabled      = :enabled,
                           args         = :args,
                           kwargs       = :kwargs,
                           options      = :options,
                           last_updated = CURRENT_TIMESTAMP
                       WHERE id = :id
                       """, {
                           'id': task_id,
                           'task': data['task'],
                           'schedule': data['schedule'],
                           'enabled': 1 if data.get('enabled', True) else 0,
                           'args': json.dumps(data.get('args', [])),
                           'kwargs': json.dumps(data.get('kwargs', {})),
                           'options': json.dumps(data.get('options', {}))
                       })

        conn.commit()

        # Perbarui jadwal Celery
        update_celery_schedule()

        return jsonify({'status': 'success'})

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Gagal memperbarui task {task_id}: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 400
    finally:
        if conn:
            conn.close()


@bp.route('/api/celery/tasks/reload', methods=['POST'])
def reload_schedule():
    """Memuat ulang jadwal Celery"""
    try:
        update_celery_schedule()
        return jsonify({'status': 'success', 'message': 'Jadwal berhasil dimuat ulang'})
    except Exception as e:
        logger.error(f"Gagal memuat ulang jadwal: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500