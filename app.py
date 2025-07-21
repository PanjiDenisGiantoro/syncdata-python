import logging

from flask import Flask, jsonify, request
from controller import get_cnote_numbers   # Import the route function
from controller import get_moda
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

# Fungsi untuk memanggil task atau API untuk mendapatkan CNOTE Numbers

def scheduled_task():
    logger.info("Scheduled task is running...")  # Logging untuk task scheduler
    try:
        # Pastikan kode dijalankan dalam konteks aplikasi Flask
        with app.app_context():  # Menggunakan konteks aplikasi Flask
            get_cnote_numbers()  # Panggil fungsi yang ada pada controller
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
            get_cnote_numbers()  # Panggil fungsi yang ada pada controller
        return jsonify({"message": "CNOTE numbers updated successfully!"}), 200
    except Exception as e:
        return jsonify({"message": f"Error occurred: {str(e)}"}), 500

@app.route("/get_moda", methods=["GET"])
def get_moda_route():
    p_date = request.args.get('p_date')  # Dapatkan parameter p_date dari query string
    if p_date:
        try:
            return get_moda(p_date)  # Panggil fungsi yang ada pada controller
        except Exception as e:
            return jsonify({"message": f"Error occurred: {str(e)}"}), 500
    else:
        return jsonify({"message": "p_date parameter is required."}), 400

# Menjalankan scheduled_task satu kali di background saat start

def run_once_job():
    logger.info("Running scheduled_task once in background thread...")
    thread = threading.Thread(target=scheduled_task)
    thread.start()
    logger.info("Job started.")

# Menjalankan Flask app

def run_flask_app():
    app.run(debug=False, use_reloader=False)  # use_reloader=False agar scheduler tidak jalan dua kali

if __name__ == "__main__":
    run_once_job()
    run_flask_app()
