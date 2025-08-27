module.exports = {
    apps: [
        {
            name: "celery-worker-flight",
            script: "celery",
            args: "-A celery_app.celery_app worker -l info -Q default --concurrency=4",
            interpreter: "none",
        },
        {
            name: "celery-worker-sync-ctc",
            script: "celery",
            args: "-A celery_app.celery_app worker -l info -Q sync-ctc --concurrency=1",
            interpreter: "none",
        },
        {
            name: "celery-beat",
            script: "celery",
            args: "-A celery_app.celery_app beat -l info",
            interpreter: "none",
        },
        {
            name: "cnote-update-schedule",
            script: "app.py",
            interpreter: "python",
            // restart_delay: 5000,
            // max_restarts: 10,
            // max_memory_restart: "300M"
        }
    ]
}
