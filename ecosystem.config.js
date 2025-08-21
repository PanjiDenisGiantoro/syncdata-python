module.exports = {
    apps: [
        {
            name: "flight-celery-worker",
            script: "celery",
            args: "-A celery_app.celery_app worker -l info --pool=solo",
            interpreter: "none",
            // restart_delay: 5000,
            // max_restarts: 10,
            // max_memory_restart: "500M"
        },
        {
            name: "flight-celery-beat",
            script: "celery",
            args: "-A celery_app.celery_app beat -l info",
            interpreter: "none",
            // restart_delay: 5000,
            // max_restarts: 10,
            // max_memory_restart: "200M"
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
