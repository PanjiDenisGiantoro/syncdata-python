from flask import Flask, jsonify
import oracledb
from config import Config

app = Flask(__name__)

# Koneksi ke database Oracle - ctcv2db
def get_oracle_connection_ctcv2db():
    try:
        connection = oracledb.connect(
            user=Config.DB_USER_CTCV2,
            password=Config.DB_PASSWORD_CTCV2,
            dsn=Config.DB_DSN_CTCV2
        )
        return connection
    except oracledb.DatabaseError as e:
        print(f"Error while connecting to Oracle ctcv2db: {e}")
        return None

# Koneksi ke database Oracle - Billing
def get_oracle_connection_billing():
    try:
        connection = oracledb.connect(
            user=Config.DB_USER_BILLING,
            password=Config.DB_PASSWORD_BILLING,
            dsn=Config.DB_DSN_BILLING
        )
        return connection
    except oracledb.DatabaseError as e:
        print(f"Error while connecting to Oracle Billing: {e}")
        return None

# Koneksi ke database Oracle - dbrbn
def get_oracle_connection_dbrbn():
    try:
        connection = oracledb.connect(
            user=Config.DB_USER_DBRBN,
            password=Config.DB_PASSWORD_DBRBN,
            dsn=Config.DB_DSN_DBRBN
        )
        return connection
    except oracledb.DatabaseError as e:
        print(f"Error while connecting to Oracle dbrbn: {e}")
        return None

# Route untuk menguji koneksi ke database ctcv2db
@app.route("/test_connection_ctcv2db")
def test_connection_ctcv2db():
    connection = get_oracle_connection_ctcv2db()
    if connection:
        connection.close()
        return jsonify({"message": "Connection to ctcv2db successful!"}), 200
    else:
        return jsonify({"message": "Connection to ctcv2db failed!"}), 500

# Route untuk menguji koneksi ke database Billing
@app.route("/test_connection_billing")
def test_connection_billing():
    connection = get_oracle_connection_billing()
    if connection:
        connection.close()
        return jsonify({"message": "Connection to Billing successful!"}), 200
    else:
        return jsonify({"message": "Connection to Billing failed!"}), 500

# Route untuk menguji koneksi ke database dbrbn
@app.route("/test_connection_dbrbn")
def test_connection_dbrbn():
    connection = get_oracle_connection_dbrbn()
    if connection:
        connection.close()
        print(f"CTCV2 User: {Config.DB_USER_CTCV2}")
        print(f"Billing DSN: {Config.DB_DSN_BILLING}")

        return jsonify({"message": "Connection to dbrbn successful!"}), 200
    else:
        print(f"CTCV2 User: {Config.DB_USER_CTCV2}")
        print(f"Billing DSN: {Config.DB_DSN_BILLING}")

        return jsonify({"message": "Connection to dbrbn failed!"}), 500

if __name__ == "__main__":
    app.run(debug=True)
