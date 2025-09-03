from flask_cors import CORS
from flask import Flask, jsonify
import threading
from logger_config import logger  as logger
import time
import uuid  # Untuk menghasilkan ID unik
import sentry_config
from schedulers.scheduler_backup import run_backup
from schedulers.scheduler_backup.backup_utils import DatabaseBackup
from schedulers.scheduler_connote_update.controller import get_cnote_numbers
from schedulers.scheduler_flight import run_schedule_flight
from schedulers.scheduler_sync_ctc import run_schedule_sync_ctc


# Fungsi untuk mengkonfigurasi logging dengan rotasi file setiap hari

app = Flask(__name__)
CORS(app)

# Fungsi untuk memanggil task atau API untuk mendapatkan CNOTE Numbers
def scheduled_task():
    job_id = str(uuid.uuid4())
    logger.info(f"Scheduled task started with Job ID: {job_id}")
    try:
        with app.app_context():
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

        return True

    except Exception as e:
        logger.error(f"Error occurred while running the task: {str(e)}")
        return True


# Home route for testing if Flask is up and running
@app.route("/", methods=["GET"])
def home():
    return "Flask app is running!"

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
            last_monitoring_run = current_time

        time.sleep(10)


if __name__ == "__main__":
    # Memulai task berulang kali di thread terpisah
    thread = threading.Thread(target=run_backup)
    thread.daemon = True  # Pastikan thread ini berhenti saat aplikasi berhenti
    thread.start()

    # scheduler_thread = threading.Thread(target=run_schedule_flight, daemon=True)
    # scheduler_thread.start()
    #
    # scheduler_thread = threading.Thread(target=DatabaseBackup, daemon=True)
    # scheduler_thread.start()
    #
    # scheduler_thread = threading.Thread(target=run_schedule_sync_ctc, daemon=True)
    # scheduler_thread.start()


    # Menjalankan Flask app
    run_flask_app()
