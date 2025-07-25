import logging
from flask import Flask, jsonify, request
from controller import get_cnote_numbers  # Import the route function
import threading
import time  # Import time module for generating unique IDs
from db import get_oracle_connection_billing
import uuid  # Untuk menghasilkan ID unik

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
    return jsonify({"scheduler_status": "not used"})

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
            job_id = str(uuid.uuid4())  # Generate unique job ID for the current request
            get_cnote_numbers(job_id)  # Panggil fungsi yang ada pada controller dengan job_id
        return jsonify({"message": "CNOTE numbers updated successfully!"}), 200
    except Exception as e:
        return jsonify({"message": f"Error occurred: {str(e)}"}), 500

# Menjalankan scheduled_task berulang kali tanpa interval
def run_continuous_jobs():
    logger.info("Running continuous tasks in background...")
    while True:
        scheduled_task()  # Menjalankan task
        time.sleep(10)  # Tunggu selama 10 detik sebelum menjalankan task berikutnya

# Menjalankan Flask app
def run_flask_app():
    app.run(debug=False, use_reloader=False)  # use_reloader=False agar scheduler tidak jalan dua kali

if __name__ == "__main__":
    # Memulai task berulang kali di thread terpisah
    thread = threading.Thread(target=run_continuous_jobs)
    thread.daemon = True  # Pastikan thread ini berhenti saat aplikasi berhenti
    thread.start()

    # Menjalankan Flask app
    run_flask_app()
