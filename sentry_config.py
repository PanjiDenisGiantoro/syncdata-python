import sentry_sdk 
from sentry_sdk.integrations.celery import CeleryIntegration
from flask import Flask

sentry_sdk.init(
    dsn="https://e1aa4aba2b7ab98cc2a7744f91ccd925@o4506467821092864.ingest.us.sentry.io/4509902539522049",
    integrations=[CeleryIntegration()],
    send_default_pii=True,
    traces_sample_rate=1.0,
    environment="production",           
)

app = Flask(__name__)