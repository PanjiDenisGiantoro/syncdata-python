import sentry_sdk 
import os
from sentry_sdk.integrations.celery import CeleryIntegration
from flask import Flask

sentry_sdk.init(
    dsn=os.getenv("SENTRY_DSN"),
    integrations=[CeleryIntegration()],
    send_default_pii=True,
    traces_sample_rate=1.0,
    environment="production",           
)

app = Flask(__name__)