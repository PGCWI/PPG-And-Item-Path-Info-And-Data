# cycle_count/scheduler.py
from apscheduler.schedulers.background import BackgroundScheduler
import logging

def init_scheduler(app, cycle_count_func):
    """
    Initialize the APScheduler job to run the cycle_count_func daily 
    based on config values (SCHEDULE_HOUR, SCHEDULE_MINUTE).
    """
    hour = app.config['SCHEDULE_HOUR']
    minute = app.config['SCHEDULE_MINUTE']

    scheduler = BackgroundScheduler()
    scheduler.add_job(
        cycle_count_func, 
        'cron', 
        hour=hour, 
        minute=minute,
        id='daily_cycle_count',
        replace_existing=True
    )
    scheduler.start()
    logging.info(f"Scheduler initialized for daily run at {hour:02d}:{minute:02d}.")
