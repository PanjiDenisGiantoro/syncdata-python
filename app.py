import logging
from logging.handlers import TimedRotatingFileHandler
import os
from flask_cors import CORS
import oracledb
from flask import Flask, jsonify, request, abort
import threading
from logger_config import logger
import time
import uuid  # Untuk menghasilkan ID unik
from controller import get_cnote_numbers  # Import the route function
from case.connote_update.p_monitoring_data_cnote import monitoring_cnote_count_today  # Import monitoring function
from db import get_oracle_connection_billing, get_oracle_connection_dbrbn
from progress_utils import load_progress

# Fungsi untuk mengkonfigurasi logging dengan rotasi file setiap hari

app = Flask(__name__)
CORS(app)


# def ensure_progress_data():
#     if progress_data is None:
#         progress_data = {
#             'total': 0,
#             'success': 0,
#             'failed': 0,
#             'status': 'Menunggu'
#         }

@app.route("/get_progress", methods=["GET"])
def get_progress():
    progress_data = load_progress()
    
    if not progress_data or 'status' not in progress_data:
        return jsonify({
            'status': 'error',
            'message': 'Progress data not available',
            'data': None
        }), 404
        
    # Calculate batch information
    total = progress_data.get('total', 0)
    success = progress_data.get('success', 0)
    failed = progress_data.get('failed', 0)
    status = progress_data.get('status', 'Tidak Diketahui')
    batch_size = progress_data.get('batch_size', 1000)
    
    # Calculate batch information
    total_batches = progress_data.get('total_batches', 0)
    current_batch = progress_data.get('current_batch', 0)
    logs = progress_data.get('logs', [])
    
    # Calculate progress percentages
    progress_percent = (success / total) * 100 if total > 0 else 0
    batch_percent = (current_batch / total_batches) * 100 if total_batches > 0 else 0

    return jsonify({
        "message": "Proses CNOTE",
        "total": total,
        "success": success,
        "failed": failed,
        "progress": f"{progress_percent:.2f}%",
        "batch_percent": f"{batch_percent:.2f}%",
        "status": status
    }), 200



# Fungsi untuk memanggil task atau API untuk mendapatkan CNOTE Numbers
def scheduled_task():
    job_id = str(uuid.uuid4())  # Generate unique job ID
    logger.info(f"Scheduled task started with Job ID: {job_id}")  # Logging untuk task scheduler
    try:
        # Pastikan kode dijalankan dalam konteks aplikasi Flask
        with app.app_context():  # Menggunakan konteks aplikasi Flask
            get_cnote_numbers(job_id)  # Panggil fungsi yang ada pada controller dengan job_id
    except Exception as e:
        logger.error(f"Error occurred while running the task: {str(e)}")


# Endpoint untuk memeriksa status scheduler (dummy, biar tidak error)
@app.route("/scheduler_status", methods=["GET"])
def scheduler_status():
    return jsonify({
        "scheduler_status": "running",
        "tasks": [
            {"name": "CNOTE Sync", "Sleep": "10 seconds"},
            {"name": "CNOTE Monitoring", "interval": "5 minutes"}
        ]
    })


# Home route for testing if Flask is up and running
@app.route("/", methods=["GET"])
def home():
    return "Flask app is running!"


@app.route("/test_connection_billing")
def test_connection_billing():
    connection = get_oracle_connection_dbrbn()
    if connection:
        connection.close()
        return jsonify({"message": "Connection to dbrbn successful!"}), 200
    else:
        return jsonify({"message": "Connection to dbrbn failed!"}), 500


@app.route("/api/listdatacn", methods=["GET"])
def list_data_cn():
    """
    API to fetch data from MONITORING_SYNC_CNOTE table
    Returns:
        JSON: List of records with MODULE, TOTAL_REBORN, TOTAL_BILLING, TOTAL_BILL_FLAG, and PERIODE
    """
    try:
        connection = get_oracle_connection_billing()
        if not connection:
            return jsonify({"error": "Database connection failed"}), 500

        cursor = connection.cursor()

        # Query to fetch data from MONITORING_SYNC_CNOTE
        query = """
        SELECT 
            MODULE, 
            TOTAL_REBORN, 
            TOTAL_BILLING,
            TOTAL_BILL_FLAG,
             PERIODE
        FROM MONITORING_SYNC_CNOTE
        ORDER BY PERIODE DESC, MODULE
        """

        cursor.execute(query)

        # Convert query results to list of dictionaries
        columns = [col[0].lower() for col in cursor.description]
        result = [dict(zip(columns, row)) for row in cursor.fetchall()]

        cursor.close()
        connection.close()

        return jsonify({"data": result, "count": len(result)}), 200

    except Exception as e:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()
        return jsonify({"error": str(e)}), 500


def get_oracle_connection_billing():
    """Helper function to get database connection"""
    try:
        connection = oracledb.connect(
            user=os.getenv('DB_USER_BILLING'),
            password=os.getenv('DB_PASSWORD_BILLING'),
            dsn=os.getenv('DB_DSN_BILLING')
        )
        return connection
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None


# The route for getting CNOTE numbers
@app.route("/get_cnote_numbers", methods=["GET"])
def get_cnote_numbers_route():
    try:
        with app.app_context():  # Ensure we are in app context
            job_id = str(uuid.uuid4())  # Generate unique job ID for the current request
            get_cnote_numbers(job_id)  # Panggil fungsi yang ada pada controller dengan job_id
        return jsonify({"message": "CNOTE numbers updated successfully!"}), 200
    except Exception as e:
        return jsonify({"message": f"Error occurred: {str(e)}"}), 500


def monitoring_task():
    """Task untuk menjalankan monitoring data CNOTE"""
    logger.info("Running monitoring task...")
    try:
        with app.app_context():
            result = monitoring_cnote_count_today()
            logger.info(f"Monitoring task completed. Result: {result}")
            return result
    except Exception as e:
        logger.error(f"Error in monitoring task: {str(e)}")
        return {"status": "error", "message": str(e)}


# Menjalankan scheduled_task berulang kali dengan interval yang berbeda
def run_continuous_jobs():
    logger.info("Running continuous tasks in background...")
    last_monitoring_run = 0

    while True:
        current_time = time.time()

        # Jalankan scheduled task (setiap 10 detik)
        scheduled_task()

        # Jalankan monitoring task setiap 5 menit (300 detik)
        if current_time - last_monitoring_run >= 50:  # 5 menit = 300 detik
            monitoring_task()
            last_monitoring_run = current_time

        time.sleep(10)  # Tunggu 10 detik sebelum iterasi berikutnya


# Menjalankan Flask app
def run_flask_app():
    app.run(debug=True, use_reloader=False)  # use_reloader=False agar scheduler tidak jalan dua kali


if __name__ == "__main__":
    # Memulai task berulang kali di thread terpisah
    thread = threading.Thread(target=run_continuous_jobs)
    thread.daemon = True  # Pastikan thread ini berhenti saat aplikasi berhenti
    thread.start()

    # Menjalankan Flask app
    run_flask_app()
