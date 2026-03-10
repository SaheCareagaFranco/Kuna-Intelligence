"""
data/connector.py — Conexión Redshift + fetch + cache
"""
import os, logging, logging.handlers
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np
import psycopg2
from dotenv import load_dotenv

Path("logs").mkdir(exist_ok=True)
_h = logging.handlers.TimedRotatingFileHandler(
    "logs/kuna.log", when="midnight", backupCount=30, encoding="utf-8")
logging.basicConfig(level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[_h, logging.StreamHandler()])
logger = logging.getLogger("kuna.connector")

load_dotenv()

REQUIRED_VARS = ["REDSHIFT_HOST","REDSHIFT_DATABASE","REDSHIFT_USER","REDSHIFT_PASSWORD"]

def _validate_env():
    missing = [v for v in REQUIRED_VARS if not os.getenv(v)]
    if missing:
        raise EnvironmentError(f"Variables faltantes en .env: {missing}")

CACHE_DIR = Path(os.getenv("CACHE_DIR","data/cache"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

def _get_connection():
    """Crea una conexión psycopg2 directa a Redshift (evita incompatibilidad SQLAlchemy 2.x)."""
    _validate_env()
    return psycopg2.connect(
        host=os.getenv("REDSHIFT_HOST"),
        port=int(os.getenv("REDSHIFT_PORT", 5439)),
        dbname=os.getenv("REDSHIFT_DATABASE"),
        user=os.getenv("REDSHIFT_USER"),
        password=os.getenv("REDSHIFT_PASSWORD"),
        connect_timeout=int(os.getenv("QUERY_TIMEOUT_SECONDS", 120)),
    )

QUERY = 'SELECT * FROM "playground"."kua_intelligence_v1" WHERE fecha_lead >= %s AND fecha_lead < %s'

DTYPE_MAP = {
    "fecha_lead":"datetime64[ns]","fecha_firma":"datetime64[ns]",
    "fecha_creacion_simulacion":"datetime64[ns]",
    "precio_auto":"float64","monto_credito":"float64","monto_financiar":"float64",
    "monto_mensualidad":"float64","pago_inicial":"float64","ingresos":"float64",
    "precio_seguro":"float64","interes_coche":"float64","simu_mensualidad":"float64",
    "simu_tasa":"float64","simu_mf":"float64","precio_auto_simulacion":"float64",
    "enganche_simulacion":"float64","pago_inicial_simulacion":"float64",
    "ciclo_dias":"float64","plazo_credito":"Int64","simu_plazo":"Int64",
    "anio_auto":"Int64","edad":"Int64","car_km":"float64","codigo_postal":"Int64",
}

def _cast_types(df):
    for col, dtype in DTYPE_MAP.items():
        if col not in df.columns: continue
        try:
            if dtype == "datetime64[ns]":
                df[col] = pd.to_datetime(df[col], errors="coerce")
            elif dtype == "Int64":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            else:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype(dtype)
        except Exception as e:
            logger.warning("No se pudo convertir %s a %s: %s", col, dtype, e)
    return df

def _enrich(df):
    df["convirtio"] = df["contract_id"].notna()
    if "fecha_firma" in df.columns:
        df["semana_firma"] = df["fecha_firma"].dt.to_period("W").astype(str)
        df["mes_firma"]    = df["fecha_firma"].dt.to_period("M").astype(str)
        df["anio_firma"]   = df["fecha_firma"].dt.year
    if "fecha_lead" in df.columns:
        df["mes_lead"]  = df["fecha_lead"].dt.to_period("M").astype(str)
    mask = df["ingresos"].notna() & (df["ingresos"] > 0) & df["monto_credito"].notna()
    df.loc[mask,"ratio_deuda_ingreso"] = (df.loc[mask,"monto_credito"] / df.loc[mask,"ingresos"]).round(2)
    df["ciclo_dias_limpio"] = df["ciclo_dias"].where(df["ciclo_dias"].between(0, 365))
    return df

def _validate_schema(df):
    required = ["fecha_lead","grupo_origen","ciclo_dias","contract_id"]
    missing  = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Columnas faltantes: {missing} — disponibles: {list(df.columns)}")
    logger.info("Schema OK — %d filas, %d cols, %d firmados",
                len(df), len(df.columns), df["contract_id"].notna().sum())

def _cache_path():
    return CACHE_DIR / f"kuna_dataset_{datetime.now().strftime('%Y%m%d')}.parquet"

def _clean_old_cache():
    cur = _cache_path()
    for f in CACHE_DIR.glob("kuna_dataset_*.parquet"):
        if f != cur:
            f.unlink(missing_ok=True)

def _fetch_month(conn, date_from, date_to):
    """Descarga un rango de fechas en una sola conexión (~3 min max)."""
    cur = conn.cursor(f"kuna_{date_from.strftime('%Y%m')}")
    cur.itersize = 50_000
    cur.execute(QUERY, (date_from.strftime("%Y-%m-%d"), date_to.strftime("%Y-%m-%d")))
    chunks, cols = [], None
    while True:
        rows = cur.fetchmany(50_000)
        if not rows:
            break
        if cols is None:
            cols = [d[0] for d in cur.description]
        chunks.append(pd.DataFrame(rows, columns=cols))
    cur.close()
    conn.commit()
    return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame(columns=cols or [])

def fetch_data(force_refresh=False):
    cache = _cache_path()
    if cache.exists() and not force_refresh:
        logger.info("Cargando cache: %s", cache.name)
        return pd.read_parquet(cache)

    # Descarga por mes para evitar timeout WLM (~20 min límite corporativo)
    from dateutil.relativedelta import relativedelta
    START = datetime(2025, 9, 1)
    END   = datetime.now().replace(day=1) + relativedelta(months=1)  # primer día del mes siguiente

    meses = []
    d = START
    while d < END:
        meses.append((d, d + relativedelta(months=1)))
        d += relativedelta(months=1)

    t0 = datetime.now()
    all_chunks = []
    for i, (d_from, d_to) in enumerate(meses, 1):
        logger.info("Descargando mes %d/%d: %s → %s", i, len(meses),
                    d_from.strftime("%Y-%m"), d_to.strftime("%Y-%m"))
        conn = None
        try:
            conn = _get_connection()
            chunk = _fetch_month(conn, d_from, d_to)
            logger.info("  %d filas", len(chunk))
            all_chunks.append(chunk)
        except Exception as e:
            logger.error("Error en mes %s: %s", d_from.strftime("%Y-%m"), e)
            raise
        finally:
            if conn:
                conn.close()

    df = pd.concat(all_chunks, ignore_index=True) if all_chunks else pd.DataFrame()
    logger.info("Query completa en %.1fs — %d filas totales", (datetime.now()-t0).total_seconds(), len(df))
    df = _cast_types(df)
    _validate_schema(df)
    df = _enrich(df)
    _clean_old_cache()
    df.to_parquet(cache, index=False)
    logger.info("Cache guardado: %s", cache.name)
    return df

def last_update():
    files = sorted(CACHE_DIR.glob("kuna_dataset_*.parquet"))
    if not files: return "Sin datos"
    from datetime import datetime as dt
    return dt.fromtimestamp(files[-1].stat().st_mtime).strftime("%d/%m/%Y %H:%M")
