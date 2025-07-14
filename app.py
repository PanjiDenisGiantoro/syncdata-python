import logging
from flask import Flask, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from controller import get_cnote_numbers  # Import the route function
import threading
import time  # Import time module for generating unique IDs
from db import get_oracle_connection_billing

# Konfigurasi logging untuk menyimpan log ke file
log_filename = 'app.log'  # Nama file log
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Menampilkan log di console
        logging.FileHandler(log_filename)  # Menyimpan log ke file
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Menyimpan instance scheduler secara global
scheduler = None


# Fungsi untuk memanggil task atau API untuk mendapatkan CNOTE Numbers
def scheduled_task():
    logger.info("Scheduled task is running...")  # Logging untuk task scheduler
    try:
        # Pastikan kode dijalankan dalam konteks aplikasi Flask
        with app.app_context():  # Menggunakan konteks aplikasi Flask
            get_cnote_numbers()  # Panggil fungsi yang ada pada controller
    except Exception as e:
        logger.error(f"Error occurred while running the task: {str(e)}")


# Scheduler setup
def start_scheduler():
    global scheduler
    logger.info("Starting scheduler...")  # Logging saat scheduler dimulai
    scheduler = BackgroundScheduler()

    # Menghasilkan ID unik dengan waktu saat job dijadwalkan
    unique_job_id = f"get_cnote_job_{int(time.time())}"  # Menambahkan timestamp untuk ID unik

    scheduler.add_job(
        func=scheduled_task,
        trigger=IntervalTrigger(minutes=2),  # Menjadwalkan setiap 2 menit
        id=unique_job_id,  # ID job unik
        name='Get CNOTE Numbers every 2 minutes',  # Nama job
        replace_existing=True
    )
    scheduler.start()
    logger.info(f"Scheduler started successfully with job ID: {unique_job_id}")


# Endpoint untuk memeriksa status scheduler
@app.route("/scheduler_status", methods=["GET"])
def scheduler_status():
    if scheduler is None:
        return jsonify({
            "scheduler_status": "not initialized",
            "jobs": []
        }), 500

    scheduler_running = False
    job_ids = []
    if scheduler.get_jobs():
        scheduler_running = True
        job_ids = [job.id for job in scheduler.get_jobs()]

    return jsonify({
        "scheduler_status": "running" if scheduler_running else "idle",
        "jobs": job_ids
    })


# Home route for testing if Flask is up and running
@app.route("/", methods=["GET"])
def home():
    return "Flask app is running!"

@app.route("/test_connection_billing")
def test_connection_billing():
    connection = get_oracle_connection_billing()
    if connection:
        connection.close()
        return jsonify({"message": "Connection to Billing successful!"}), 200
    else:
        return jsonify({"message": "Connection to Billing failed!"}), 500


# The route for getting CNOTE numbers
@app.route("/get_cnote_numbers", methods=["GET"])
def get_cnote_numbers_route():
    try:
        with app.app_context():  # Ensure we are in app context
            get_cnote_numbers()  # Panggil fungsi yang ada pada controller
        return jsonify({"message": "CNOTE numbers updated successfully!"}), 200
    except Exception as e:
        return jsonify({"message": f"Error occurred: {str(e)}"}), 500


# Menjalankan scheduler di background
def run_flask_app():
    app.run(debug=True, use_reloader=False)  # use_reloader=False agar scheduler tidak jalan dua kali


if __name__ == "__main__":
    # Memulai scheduler di thread terpisah
    scheduler_thread = threading.Thread(target=start_scheduler)
    scheduler_thread.start()
    # Menjalankan Flask app
    run_flask_app()
