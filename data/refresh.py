"""
data/refresh.py — Scheduler de actualización diaria
"""
import os, logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED

logger = logging.getLogger("kuna.scheduler")

def _listener(event):
    if event.exception:
        logger.error("Actualizacion automatica fallo: %s", event.exception)
    else:
        logger.info("Actualizacion automatica completada")

def daily_refresh():
    from data.connector import fetch_data
    logger.info("Iniciando actualizacion automatica...")
    fetch_data(force_refresh=True)

def start_scheduler():
    hour   = int(os.getenv("REFRESH_HOUR", 6))
    minute = int(os.getenv("REFRESH_MINUTE", 0))
    s = BackgroundScheduler(timezone="America/Mexico_City")
    s.add_job(daily_refresh, trigger="cron", hour=hour, minute=minute,
              id="daily_refresh", replace_existing=True, misfire_grace_time=3600)
    s.add_listener(_listener, EVENT_JOB_ERROR | EVENT_JOB_EXECUTED)
    s.start()
    logger.info("Scheduler activo — actualizacion diaria %02d:%02d CDMX", hour, minute)
    return s
