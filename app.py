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
from controller import get_cnote_numbers, get_update_moda ,get_flight # Import the route function
from case.connote_update.p_monitoring_data_cnote import monitoring_cnote_count_today  # Import monitoring function
from db import get_oracle_connection_billing, get_oracle_connection_dbrbn, get_oracle_connection_training
from progress_utils import load_progress
from datetime import datetime
from scheduler import run_schedule_flight

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
    batch_size = progress_data.get('batch_size', 500)
    
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
        "status": status,
        "batch_size": batch_size,
        "total_batches": total_batches,
        "current_batch": current_batch,
        "logs": logs
    }), 200



# Fungsi untuk memanggil task atau API untuk mendapatkan CNOTE Numbers
def scheduled_task(run_flight=False):
    job_id = str(uuid.uuid4())
    logger.info(f"Scheduled task started with Job ID: {job_id}")
    try:
        with app.app_context():
            # Proses CNOTE Numbers
            try:
                response = get_cnote_numbers(job_id)
                if isinstance(response, tuple):
                    response_data, status_code = response
                    if (
                        status_code == 404
                        and response_data.get_json().get("message") == "No CNOTE numbers found."
                    ):
                        logger.info(f"Job ID {job_id}: Skipping CNOTE processing (no data).")
            except Exception as e:
                logger.error(f"Job ID {job_id}: Error in get_cnote_numbers - {str(e)}")

            # Proses Flight hanya jika flag True
            if run_flight:
                try:
                    flight_response = get_flight(job_id)
                    if isinstance(flight_response, tuple):
                        flight_data, flight_status = flight_response
                        if (
                            flight_status == 404
                            and flight_data.get_json().get("message") == "No flight data found."
                        ):
                            logger.info(f"Job ID {job_id}: Skipping flight processing (no data).")
                except Exception as e:
                    logger.error(f"Job ID {job_id}: Error in get_flight - {str(e)}")

        return True

    except Exception as e:
        logger.error(f"Error occurred while running the task: {str(e)}")
        return True




# Endpoint untuk memeriksa status scheduler (dummy, biar tidak error)
@app.route("/scheduler_status", methods=["GET"])
def scheduler_status():
    return jsonify({
        "scheduler_status": "running",
        "tasks": [
            {"name": "CNOTE Sync", "Sleep": "10 seconds"},
            {"name": "CNOTE Monitoring", "interval": "5 minutes"},
            {"name": "Flight API", "interval": "5 minutes"}
        ]
    })


# Home route for testing if Flask is up and running
@app.route("/", methods=["GET"])
def home():
    return "Flask app is running!"


@app.route("/test_connection_billing")
def test_connection_training():
    connection = get_oracle_connection_training()
    if connection:
        connection.close()
        return jsonify({"message": "Connection to dbrbn successful!"}), 200
    else:
        return jsonify({"message": "Connection to training failed!"}), 500

@app.route("/api/flight")
def flight():
    job_id = str(uuid.uuid4())  # Generate unique job ID for the current request
    get_flight(job_id)
    return jsonify({"message": "Flight data updated successfully!"}), 200


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
             PERIODE,
            TOTAL_CNOTE_UPDATE
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

@app.route("/get_upd_moda", methods=["GET"])
def get_update_moda_route():
    try:
        with app.app_context():  # Ensure we are in app context
            job_id = str(uuid.uuid4())  # Generate unique job ID for the current request
            get_update_moda(job_id)  # Panggil fungsi yang ada pada controller dengan job_id
        return jsonify({"message": "update moda successfully!"}), 200
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

def run_continuous_jobs():
    logger.info("Running continuous tasks in background...")

    last_monitoring_run = 0
    last_flight_run_date = None  # Menyimpan tanggal terakhir get_flight dijalankan

    while True:
        current_time = time.time()
        today_str = datetime.now().strftime("%Y-%m-%d")  # Tanggal hari ini

        # 🔴 Stop loop jika tidak ada data untuk diproses
        should_continue = scheduled_task(run_flight=(last_flight_run_date != today_str))
        if not should_continue:
            logger.info("No CNOTE to process. Stopping background task loop.")
            break

        # Kalau hari berganti, tandai sudah jalan
        if last_flight_run_date != today_str:
            last_flight_run_date = today_str

        # Monitoring task tiap 5 menit
        if current_time - last_monitoring_run >= 300:
            monitoring_task()
            last_monitoring_run = current_time

        time.sleep(10)


@app.route("/restart_scheduler", methods=["POST"])
def restart_scheduler():
    thread = threading.Thread(target=run_continuous_jobs)
    thread.daemon = True
    thread.start()
    return jsonify({"message": "Scheduler restarted"}), 200


# Menjalankan Flask app
def run_flask_app():
    app.run(host='0.0.0.0',debug=True, use_reloader=False, port=5001)  # use_reloader=False agar scheduler tidak jalan dua kali

stop_signal = False

def run_continuous_jobs():
    global stop_signal
    stop_signal = False  # reset saat mulai
    logger.info("Running continuous tasks in background...")
    last_monitoring_run = 0

    while not stop_signal:
        current_time = time.time()

        should_continue = scheduled_task()
        if not should_continue:
            logger.info("No CNOTE to process. Stopping background task loop.")
            break

        if current_time - last_monitoring_run >= 300:
            monitoring_task()
            last_monitoring_run = current_time

        time.sleep(10)

@app.route("/stop_scheduler", methods=["POST"])
def stop_scheduler():
    global stop_signal
    stop_signal = True
    return jsonify({"message": "Scheduler stop requested"}), 200


if __name__ == "__main__":
    # Memulai task berulang kali di thread terpisah
    thread = threading.Thread(target=run_continuous_jobs)
    thread.daemon = True  # Pastikan thread ini berhenti saat aplikasi berhenti
    thread.start()

    scheduler_thread = threading.Thread(target=run_schedule_flight)
    scheduler_thread.daemon = True
    scheduler_thread.start()

    # Menjalankan Flask app
    run_flask_app()
