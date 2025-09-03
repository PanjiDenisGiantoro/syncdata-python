# app.py
from flask import Flask
from api_celery import bp as celery_bp
from flask_cors import CORS
import time
import uuid
app = Flask(__name__)
CORS(app)

# Daftarkan blueprint
app.register_blueprint(celery_bp)

if __name__ == "__main__":
    app.run(debug=True)