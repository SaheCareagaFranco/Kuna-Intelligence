"""
data/refresh.py — Scheduler de actualización diaria

Flujo diario (default 09:00 CDMX):
  1. Reconstruye playground.kua_intelligence_v1 en Redshift (~90s)
  2. Descarga el resultado y guarda cache local en parquet
Así la web carga en <1s leyendo el parquet en lugar de golpear Redshift.
"""
import os, logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED

logger = logging.getLogger("kuna.scheduler")

def _listener(event):
    if event.exception:
        logger.error("Actualizacion automatica fallo: %s", event.exception)
    else:
        logger.info("Actualizacion automatica completada OK")

def daily_refresh():
    """Paso 1: reconstruye tabla Redshift. Paso 2: actualiza cache parquet."""
    import refresh_table
    from data.connector import fetch_data

    logger.info("[1/2] Reconstruyendo playground.kua_intelligence_v1 en Redshift...")
    refresh_table.refresh()

    logger.info("[2/2] Descargando datos y actualizando cache local...")
    fetch_data(force_refresh=True)

    logger.info("Actualizacion diaria completada.")

def start_scheduler():
    hour   = int(os.getenv("REFRESH_HOUR", 9))
    minute = int(os.getenv("REFRESH_MINUTE", 0))
    s = BackgroundScheduler(timezone="America/Mexico_City")
    s.add_job(daily_refresh, trigger="cron", hour=hour, minute=minute,
              id="daily_refresh", replace_existing=True, misfire_grace_time=3600)
    s.add_listener(_listener, EVENT_JOB_ERROR | EVENT_JOB_EXECUTED)
    s.start()
    logger.info("Scheduler activo — actualizacion diaria %02d:%02d CDMX", hour, minute)
    return s
