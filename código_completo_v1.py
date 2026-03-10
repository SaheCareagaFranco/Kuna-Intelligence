"""
instalar_kuna.py
────────────────────────────────────────────────────────────────────────────────
Crea toda la estructura del proyecto Kuna Intelligence en una sola ejecución.

Uso:
    python instalar_kuna.py

Crea todo en:
    C:/Users/sahe.careaga_kavak/Documents/Análisis/Análisis_Python/Claude/Intranet_Analysis/
"""

import os
import sys
from pathlib import Path

BASE = Path(r"C:\Users\sahe.careaga_kavak\Documents\Análisis\Análisis_Python\Claude\Intranet_Analysis")

# ─── Colores ──────────────────────────────────────────────────────────────────
G = "\033[92m"; Y = "\033[93m"; R = "\033[91m"; B = "\033[0m"
ok   = lambda m: print(f"  {G}✓{B}  {m}")
info = lambda m: print(f"  {Y}→{B}  {m}")

# ─── Archivos del proyecto ────────────────────────────────────────────────────
FILES = {}

# ══════════════════════════════════════════════════════════════════════════════
FILES[".env"] = r"""# Redshift
REDSHIFT_HOST=redshift-private.de.prod.kavak.services
REDSHIFT_PORT=5439
REDSHIFT_DATABASE=prod
REDSHIFT_USER=sahe_careaga
REDSHIFT_PASSWORD=2>91B$Xuv06P

# App
REFRESH_HOUR=6
REFRESH_MINUTE=0
CACHE_DIR=data/cache
QUERY_TIMEOUT_SECONDS=120
LOG_LEVEL=INFO
"""

# ══════════════════════════════════════════════════════════════════════════════
FILES[".env.example"] = """# Redshift
REDSHIFT_HOST=your-cluster.xxxxxx.us-east-1.redshift.amazonaws.com
REDSHIFT_PORT=5439
REDSHIFT_DATABASE=your_database
REDSHIFT_USER=your_readonly_user
REDSHIFT_PASSWORD=your_password

# App
REFRESH_HOUR=6
REFRESH_MINUTE=0
CACHE_DIR=data/cache
QUERY_TIMEOUT_SECONDS=120
LOG_LEVEL=INFO
"""

# ══════════════════════════════════════════════════════════════════════════════
FILES[".gitignore"] = """.env
data/cache/
logs/
__pycache__/
*.pyc
.venv/
venv/
.streamlit/secrets.toml
*.egg-info/
.pytest_cache/
.coverage
"""

# ══════════════════════════════════════════════════════════════════════════════
FILES["requirements.txt"] = """streamlit>=1.35.0
pandas>=2.0.0
numpy>=1.26.0
plotly>=5.20.0
sqlalchemy>=2.0.0
psycopg2-binary>=2.9.9
python-dotenv>=1.0.0
pyarrow>=14.0.0
apscheduler>=3.10.0
pytest>=7.4.0
pytest-cov>=4.1.0
"""

# ══════════════════════════════════════════════════════════════════════════════
FILES["data/__init__.py"] = ""
FILES["metrics/__init__.py"] = ""
FILES["tests/__init__.py"] = ""

# ══════════════════════════════════════════════════════════════════════════════
FILES["data/connector.py"] = '''"""
data/connector.py — Conexión Redshift + fetch + cache
"""
import os, logging, logging.handlers
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, SQLAlchemyError
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

def get_engine():
    _validate_env()
    conn_str = (
        f"postgresql+psycopg2://{os.getenv(\'REDSHIFT_USER\')}:{os.getenv(\'REDSHIFT_PASSWORD\')}"
        f"@{os.getenv(\'REDSHIFT_HOST\')}:{os.getenv(\'REDSHIFT_PORT\',5439)}/{os.getenv(\'REDSHIFT_DATABASE\')}"
    )
    try:
        engine = create_engine(conn_str,
            connect_args={"connect_timeout": int(os.getenv("QUERY_TIMEOUT_SECONDS",120))},
            pool_pre_ping=True, pool_recycle=3600)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Conexion a Redshift OK — %s/%s", os.getenv("REDSHIFT_HOST"), os.getenv("REDSHIFT_DATABASE"))
        return engine
    except OperationalError as e:
        logger.error("Error de conexion Redshift: %s", e)
        raise

QUERY = """
WITH leads AS (
    SELECT
        convert_timezone(\'America/Mexico_City\', create_date) AS fecha_lead,
        financing_application_id,
        user_id,
        vehicle_id AS asset,
        ksan.grupo                   AS grupo_origen,
        ksan.nombre_completo_agencia AS agencia_origen,
        ksan.proyecto_f              AS proyecto_origen
    FROM financing_acceptation_api_global_refined.financing_leads_data fld
    LEFT JOIN playground.kua_sensitive_ao_nexus ksan ON fld.agency_id = ksan.agency_id
    WHERE country_id = \'484\'
      AND convert_timezone(\'America/Mexico_City\', create_date) >= \'2025-09-01\'
),
contratos AS (
    SELECT
        convert_timezone(\'America/Mexico_City\', m."date") AS fecha_firma,
        m.contract_id,
        json_extract_path_text(m."data",\'client\',\'uuid\')                              AS user_id,
        json_extract_path_text(m."data",\'car\',\'make\')                                 AS marca_auto,
        CAST(json_extract_path_text(m."data",\'car\',\'value\')      AS DECIMAL(12,2))    AS precio_auto,
        CAST(json_extract_path_text(m."data",\'car\',\'year\')       AS INTEGER)          AS anio_auto,
        json_extract_path_text(m."data",\'car\',\'model\')                                AS modelo_auto,
        json_extract_path_text(m."data",\'car\',\'version\')                              AS version_auto,
        CAST(json_extract_path_text(m."data",\'car\',\'km\')         AS NUMERIC)          AS car_km,
        json_extract_path_text(m."data",\'car\',\'condition\')                            AS tipo_auto,
        CAST(json_extract_path_text(m."data",\'car\',\'interest\')   AS DECIMAL(10,2))    AS interes_coche,
        json_extract_path_text(m."data",\'insurance\',\'name\')                           AS proveedor_seguro,
        CAST(json_extract_path_text(m."data",\'insurance\',\'price\') AS DECIMAL(10,2))   AS precio_seguro,
        json_extract_path_text(m."data",\'insurance\',\'insurancePaymentMethod\')         AS metodo_pago_seguro,
        CAST(json_extract_path_text(m."data",\'credit\',\'amount\')  AS DECIMAL(12,2))    AS monto_credito,
        CAST(CAST(json_extract_path_text(m."data",\'car\',\'financingAmount\') AS DECIMAL(12,0)) AS INTEGER) AS monto_financiar,
        CAST(json_extract_path_text(m."data",\'credit\',\'monthlyPayment\') AS DECIMAL(10,2))    AS monto_mensualidad,
        CAST(CAST(json_extract_path_text(m."data",\'credit\',\'installments\') AS DECIMAL(10,0)) AS INTEGER) AS plazo_credito,
        CAST(json_extract_path_text(m."data",\'credit\',\'initialPayment\')  AS DECIMAL(12,2))   AS pago_inicial,
        CAST(json_extract_path_text(m."data",\'client\',\'profiling\',\'income\') AS DECIMAL(12,2)) AS ingresos,
        CAST(CAST(json_extract_path_text(m."data",\'client\',\'address\',\'zipCode\') AS DECIMAL(10,0)) AS INTEGER) AS codigo_postal,
        DATEDIFF(year, CAST(json_extract_path_text(m."data",\'client\',\'birthday\') AS DATE), CURRENT_DATE) AS edad,
        json_extract_path_text(m."data",\'client\',\'bank\',\'bank\')                      AS banco_cliente,
        CAST(CAST(json_extract_path_text(m."data",\'car\',\'stockId\') AS DECIMAL(10,0)) AS INTEGER) AS asset,
        json_extract_path_text(m."data",\'credit\',\'branch\',\'deal\')                    AS proyecto,
        json_extract_path_text(m."data",\'credit\',\'branch\',\'agencyId\')                AS agency_id
    FROM financing_contract_api_global_refined.metrics AS m
    WHERE upper(m.status) = \'CONTRATO FIRMADO\'
      AND json_extract_path_text(m."data",\'credit\',\'branch\',\'deal\') = \'NRFM\'
      AND convert_timezone(\'America/Mexico_City\', m."date") >= \'2025-09-01\'::DATE
),
base_simus AS (
    SELECT
        CONVERT_TIMEZONE(\'America/Mexico_City\', cso.created_at) AS fecha_creacion_simulacion,
        cso.user_id, s.offer_id,
        CAST(cso.installments::numeric AS INT) AS plazo,
        ROUND(cso.installment_amount, 2)        AS mensualidad
    FROM financing_products_api_global_refined.ca_simulation_option cso
    JOIN financing_products_api_global_refined.fpm_simulation s ON cso.simulation_id = s.id
    JOIN financing_products_api_global_refined.fpm_simulation_request fsr ON fsr.simulation_id = s.id
    WHERE cso.status = \'CONFIRMED\'
),
simus_enriquecidas AS (
    SELECT
        b.fecha_creacion_simulacion, b.user_id, b.plazo, b.mensualidad,
        ffr.annual_rate AS tasa, ffr.financed_amount AS mf,
        ROUND(json_extract_path_text(ffr.details,\'initialPeriod\',\'car\',\'balance\')::NUMERIC,2) AS precio_auto_simulacion,
        ROUND(json_extract_path_text(ffr.details,\'initialPeriod\',\'car\',\'capital\')::NUMERIC,2) AS enganche_simulacion,
        ROUND(json_extract_path_text(ffr.details,\'initialPeriod\',\'total\')::NUMERIC,2)          AS pago_inicial_simulacion,
        json_extract_path_text(ffr.details,\'downPaymentRate\')                                     AS enganche_pct_simulacion,
        TRY_CAST(ffr.vehicle_id AS INTEGER) AS asset
    FROM base_simus b
    LEFT JOIN financing_products_api_global_refined.fpm_financial_run ffr ON ffr.offer_id = b.offer_id
    WHERE b.fecha_creacion_simulacion >= \'2025-09-01\'
),
ultima_simu AS (
    SELECT * FROM simus_enriquecidas
    QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id, asset ORDER BY fecha_creacion_simulacion DESC) = 1
)
SELECT
    l.fecha_lead, l.proyecto_origen, l.grupo_origen, l.agencia_origen,
    s.fecha_creacion_simulacion,
    s.plazo AS simu_plazo, s.mensualidad AS simu_mensualidad,
    s.tasa AS simu_tasa, s.mf AS simu_mf,
    s.precio_auto_simulacion, s.enganche_simulacion,
    s.pago_inicial_simulacion, s.enganche_pct_simulacion,
    c.fecha_firma, c.contract_id,
    c.marca_auto, c.precio_auto, c.anio_auto, c.modelo_auto, c.version_auto,
    c.car_km, c.tipo_auto, c.interes_coche,
    c.proveedor_seguro, c.precio_seguro, c.metodo_pago_seguro,
    c.monto_credito, c.monto_financiar, c.monto_mensualidad,
    c.plazo_credito, c.pago_inicial, c.ingresos,
    c.codigo_postal, c.edad, c.banco_cliente, c.proyecto, c.agency_id,
    DATEDIFF(day, l.fecha_lead, c.fecha_firma) AS ciclo_dias
FROM leads AS l
LEFT JOIN contratos AS c ON c.contract_id = l.financing_application_id
LEFT JOIN ultima_simu AS s ON l.user_id = s.user_id AND l.asset = s.asset
ORDER BY c.fecha_firma DESC NULLS LAST;
"""

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
    return CACHE_DIR / f"kuna_dataset_{datetime.now().strftime(\'%Y%m%d\')}.parquet"

def _clean_old_cache():
    cur = _cache_path()
    for f in CACHE_DIR.glob("kuna_dataset_*.parquet"):
        if f != cur:
            f.unlink(missing_ok=True)

def fetch_data(force_refresh=False):
    cache = _cache_path()
    if cache.exists() and not force_refresh:
        logger.info("Cargando cache: %s", cache.name)
        return pd.read_parquet(cache)
    logger.info("Consultando Redshift...")
    t0 = datetime.now()
    try:
        engine = get_engine()
        df = pd.read_sql(QUERY, engine)
        logger.info("Query OK en %.1fs — %d filas", (datetime.now()-t0).total_seconds(), len(df))
    except SQLAlchemyError as e:
        logger.error("Error en query: %s", e)
        raise
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
'''

# ══════════════════════════════════════════════════════════════════════════════
FILES["data/refresh.py"] = '''"""
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
'''

# ══════════════════════════════════════════════════════════════════════════════
FILES["metrics/__init__.py"] = ""
FILES["metrics/ciclo_venta.py"] = '''"""
metrics/ciclo_venta.py — Cálculos estadísticos del ciclo de venta
"""
import numpy as np
import pandas as pd

def stats_ciclo(df, col="ciclo_dias_limpio"):
    s = df[col].dropna() if col in df.columns else pd.Series(dtype=float)
    if s.empty:
        return {k: None for k in ["n","mean","median","std","min","max","p10","q1","q3","p90","iqr","skewness","kurtosis"]}
    return {
        "n":        int(len(s)),
        "mean":     round(float(s.mean()), 2),
        "median":   round(float(s.median()), 2),
        "std":      round(float(s.std()), 2),
        "min":      round(float(s.min()), 1),
        "max":      round(float(s.max()), 1),
        "p10":      round(float(s.quantile(.10)), 2),
        "q1":       round(float(s.quantile(.25)), 2),
        "q3":       round(float(s.quantile(.75)), 2),
        "p90":      round(float(s.quantile(.90)), 2),
        "iqr":      round(float(s.quantile(.75) - s.quantile(.25)), 2),
        "skewness": round(float(s.skew()), 3),
        "kurtosis": round(float(s.kurtosis()), 3),
    }

def kpis_principales(df):
    firmados = df[df["convirtio"]] if "convirtio" in df.columns else df[df["contract_id"].notna()]
    total  = len(df)
    n_firm = len(firmados)
    ciclo  = firmados["ciclo_dias_limpio"].dropna() if "ciclo_dias_limpio" in firmados.columns else pd.Series()
    monto  = firmados["monto_credito"].dropna()     if "monto_credito" in firmados.columns else pd.Series()
    ingreso= firmados["ingresos"].dropna()           if "ingresos" in firmados.columns else pd.Series()
    ratio  = firmados["ratio_deuda_ingreso"].dropna()if "ratio_deuda_ingreso" in firmados.columns else pd.Series()
    plazo  = firmados["plazo_credito"].dropna()      if "plazo_credito" in firmados.columns else pd.Series()
    return {
        "total_leads":    total,
        "contratos":      n_firm,
        "conversion_pct": round(n_firm/total*100,1) if total else 0,
        "ciclo_mean":     round(float(ciclo.mean()),1)   if len(ciclo) else None,
        "ciclo_median":   round(float(ciclo.median()),1) if len(ciclo) else None,
        "ciclo_std":      round(float(ciclo.std()),1)    if len(ciclo) else None,
        "monto_mean":     round(float(monto.mean()),0)   if len(monto) else None,
        "ingreso_mean":   round(float(ingreso.mean()),0) if len(ingreso) else None,
        "ratio_di_mean":  round(float(ratio.mean()),2)   if len(ratio) else None,
        "plazo_mean":     round(float(plazo.mean()),0)   if len(plazo) else None,
    }

def tendencia_semanal(df):
    firmados = df[df["convirtio"]].copy() if "convirtio" in df.columns else df[df["contract_id"].notna()].copy()
    if "semana_firma" not in firmados.columns or firmados["semana_firma"].isna().all():
        return pd.DataFrame()
    agg = firmados.groupby("semana_firma").agg(
        contratos=("contract_id","count"),
        ciclo_prom=("ciclo_dias_limpio","mean"),
        ciclo_med=("ciclo_dias_limpio","median"),
        monto_prom=("monto_credito","mean"),
    ).reset_index().sort_values("semana_firma")
    agg["ciclo_prom"]    = agg["ciclo_prom"].round(1)
    agg["monto_prom"]    = agg["monto_prom"].round(0)
    agg["contratos_wow"] = agg["contratos"].pct_change().round(3)*100
    return agg

def ciclo_por_dimension(df, dimension):
    firmados = df[df["convirtio"]].copy() if "convirtio" in df.columns else df[df["contract_id"].notna()].copy()
    if dimension not in firmados.columns:
        return pd.DataFrame()
    r = (firmados.dropna(subset=[dimension,"ciclo_dias_limpio"])
         .groupby(dimension)["ciclo_dias_limpio"]
         .agg(n="count",mean="mean",median="median",std="std",
              q1=lambda x: x.quantile(.25), q3=lambda x: x.quantile(.75),
              p90=lambda x: x.quantile(.90))
         .reset_index().round(2).sort_values("mean"))
    r["iqr"] = (r["q3"]-r["q1"]).round(2)
    return r
'''

# ══════════════════════════════════════════════════════════════════════════════
FILES["check_connection.py"] = '''"""
check_connection.py — Verifica conexion a Redshift antes de arrancar la app
Uso: python check_connection.py
"""
import os, sys, socket, time
from dotenv import load_dotenv

load_dotenv()
G="\033[92m"; R="\033[91m"; Y="\033[93m"; B="\033[94m"; E="\033[0m"; BD="\033[1m"
ok   = lambda m: print(f"  {G}OK{E}  {m}")
fail = lambda m: print(f"  {R}XX{E}  {m}")
warn = lambda m: print(f"  {Y}!!{E}  {m}")
info = lambda m: print(f"  {B}->{E}  {m}")
errors = []

print(f"\\n{BD}{'='*54}{E}")
print(f"{BD}  Kuna Intelligence — Verificacion de conexion{E}")
print(f"{BD}{'='*54}{E}")

# 1. Variables de entorno
print(f"\\n{BD}[1] Variables de entorno{E}")
VARS = {k: os.getenv(k) for k in ["REDSHIFT_HOST","REDSHIFT_PORT","REDSHIFT_DATABASE","REDSHIFT_USER","REDSHIFT_PASSWORD"]}
for k,v in VARS.items():
    if v: ok(f"{k} = {\'***\' if \'PASSWORD\' in k else v}")
    else: fail(f"{k} no definida"); errors.append(k)
if errors: print(f"\\n{R}Faltan variables.{E}"); sys.exit(1)

HOST=VARS["REDSHIFT_HOST"]; PORT=int(VARS.get("REDSHIFT_PORT",5439))
DB=VARS["REDSHIFT_DATABASE"]; USER=VARS["REDSHIFT_USER"]; PWD=VARS["REDSHIFT_PASSWORD"]

# 2. TCP
print(f"\\n{BD}[2] Conectividad TCP{E}")
try:
    t0=time.time()
    s=socket.create_connection((HOST,PORT),timeout=10); s.close()
    ok(f"Puerto {PORT} alcanzable en {(time.time()-t0)*1000:.0f}ms")
except Exception as e:
    fail(f"No se puede conectar a {HOST}:{PORT} — {e}")
    warn("Verifica que estes conectado a la VPN de Kavak")
    errors.append("TCP"); print(f"\\n{R}Sin conectividad.{E}"); sys.exit(1)

# 3. Auth
print(f"\\n{BD}[3] Autenticacion{E}")
try:
    from sqlalchemy import create_engine, text
    engine = create_engine(
        f"postgresql+psycopg2://{USER}:{PWD}@{HOST}:{PORT}/{DB}",
        connect_args={"connect_timeout":30}, pool_pre_ping=True)
    with engine.connect() as conn:
        row = conn.execute(text("SELECT current_user, current_database()")).fetchone()
    ok(f"Login como: {row[0]} en BD: {row[1]}")
except Exception as e:
    fail(f"Error de autenticacion: {e}"); errors.append("AUTH")
    print(f"\\n{R}No se pudo autenticar.{E}"); sys.exit(1)

# 4. Tablas
print(f"\\n{BD}[4] Acceso a tablas{E}")
TABLES=[
    ("financing_acceptation_api_global_refined","financing_leads_data"),
    ("financing_contract_api_global_refined","metrics"),
    ("financing_products_api_global_refined","ca_simulation_option"),
    ("financing_products_api_global_refined","fpm_simulation"),
    ("financing_products_api_global_refined","fpm_financial_run"),
    ("playground","kua_sensitive_ao_nexus"),
]
with engine.connect() as conn:
    for schema,table in TABLES:
        try:
            cnt = conn.execute(text(f\'SELECT COUNT(*) FROM "{schema}"."{table}"\' )).scalar()
            ok(f"{schema}.{table}  ({cnt:,} filas)")
        except Exception as e:
            fail(f"{schema}.{table}  — {str(e)[:70]}")
            errors.append(f"{schema}.{table}")

# 5. Query COUNT
print(f"\\n{BD}[5] Validacion de query maestra{E}")
try:
    t0=time.time()
    with engine.connect() as conn:
        row=conn.execute(text("""
            SELECT
                (SELECT COUNT(*) FROM financing_acceptation_api_global_refined.financing_leads_data
                 WHERE country_id=\'484\' AND convert_timezone(\'America/Mexico_City\',create_date)>=\'2025-09-01\') AS leads,
                (SELECT COUNT(*) FROM financing_contract_api_global_refined.metrics
                 WHERE upper(status)=\'CONTRATO FIRMADO\'
                   AND json_extract_path_text("data",\'credit\',\'branch\',\'deal\')=\'NRFM\'
                   AND convert_timezone(\'America/Mexico_City\',"date")>=\'2025-09-01\'::DATE) AS contratos
        """)).fetchone()
    ok(f"Query ejecutada en {time.time()-t0:.1f}s")
    ok(f"Leads encontrados:      {row[0]:,}")
    ok(f"Contratos NRFM:         {row[1]:,}")
    if row[0]: info(f"Conversion estimada:    {row[1]/row[0]*100:.1f}%")
except Exception as e:
    fail(f"Error en query: {e}"); errors.append("QUERY")

# Resultado final
print(f"\\n{BD}{'='*54}{E}")
if not errors:
    print(f"{G}{BD}  TODO OK — Arranca la app con:{E}")
    print(f"\\n  {BD}streamlit run app.py{E}\\n")
else:
    print(f"{R}{BD}  {len(errors)} problema(s) encontrados.{E}")
    for e in errors: print(f"  - {e}")
print(f"{BD}{'='*54}{E}\\n")
'''

# ══════════════════════════════════════════════════════════════════════════════
# app.py — entry point completo (importa desde los módulos)
FILES["app.py"] = '''"""
app.py — Kuna Intelligence entry point
Uso: streamlit run app.py
"""
import logging, logging.handlers
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from data.connector import fetch_data, last_update
from data.refresh import start_scheduler
from metrics.ciclo_venta import stats_ciclo, kpis_principales, tendencia_semanal, ciclo_por_dimension

# ── Logging ───────────────────────────────────────────────────────────────────
Path("logs").mkdir(exist_ok=True)
_h = logging.handlers.TimedRotatingFileHandler(
    "logs/kuna.log", when="midnight", backupCount=30, encoding="utf-8")
logging.basicConfig(level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[_h, logging.StreamHandler()])
logger = logging.getLogger("kuna.app")

# ── Brand ─────────────────────────────────────────────────────────────────────
G="#1AC77C"; G2="#1DE08C"; NAVY="#002236"; NAVY2="#00304C"; AMBER="#E8A838"; NEUTRAL="#748C86"
PALETTE=[G,NAVY,AMBER,G2,"#0074B4",NEUTRAL,"#C0CEC9"]

st.set_page_config(page_title="Kuna Intelligence", page_icon="🟢",
                   layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<link href="https://fonts.googleapis.com/css2?family=Outfit:wght@400;600;700;800;900&family=Noto+Sans:wght@400;500;600&display=swap" rel="stylesheet"/>
<style>
html,body,[class*="css"]{font-family:\'Noto Sans\',sans-serif}
h1,h2,h3,.stMetric label,[data-testid="stMetricValue"]{font-family:\'Outfit\',sans-serif!important}
[data-testid="stSidebar"]{background:#171D1C!important;border-right:1px solid #2C3533}
[data-testid="stSidebar"] *{color:#C0CEC9!important}
[data-testid="stSidebar"] .stRadio label{color:#C0CEC9!important;font-size:.84rem}
.kpi-card{background:white;border-radius:12px;padding:20px 18px;border-left:4px solid #1AC77C;box-shadow:0 1px 8px rgba(23,29,28,.07);margin-bottom:4px}
.kpi-label{font-family:\'Outfit\',sans-serif;font-size:.62rem;text-transform:uppercase;letter-spacing:.1em;color:#748C86;margin-bottom:6px;font-weight:600}
.kpi-value{font-family:\'Outfit\',sans-serif;font-size:1.9rem;font-weight:900;color:#171D1C;line-height:1;letter-spacing:-.03em}
.kpi-meta{font-size:.68rem;color:#748C86;margin-top:5px}
.kpi-badge{background:rgba(26,199,124,.12);color:#1AC77C;font-weight:700;font-size:.62rem;padding:2px 7px;border-radius:20px;margin-left:4px}
.stat-strip{display:flex;background:white;border-radius:12px;overflow:hidden;box-shadow:0 1px 8px rgba(23,29,28,.07);margin-bottom:16px}
.stat-item{flex:1;text-align:center;padding:13px 0;border-right:1px solid #E0E7E4}
.stat-item:last-child{border-right:none}
.stat-val{font-family:\'Outfit\',sans-serif;font-size:1.1rem;font-weight:800;color:#171D1C}
.stat-val.green{color:#1AC77C}
.stat-lbl{font-size:.6rem;text-transform:uppercase;letter-spacing:.08em;color:#748C86;margin-top:3px}
.section-title{font-family:\'Outfit\',sans-serif;font-size:.72rem;font-weight:700;text-transform:uppercase;letter-spacing:.1em;color:#748C86;margin:20px 0 10px;padding-left:2px}
.stButton>button{background:#1AC77C!important;color:white!important;border:none!important;border-radius:8px!important;font-family:\'Outfit\',sans-serif!important;font-weight:700!important}
</style>
""", unsafe_allow_html=True)

# ── Scheduler ─────────────────────────────────────────────────────────────────
if "scheduler_started" not in st.session_state:
    start_scheduler()
    st.session_state["scheduler_started"] = True

# ── Helpers ───────────────────────────────────────────────────────────────────
PLOT_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Noto Sans", color="#485856", size=11),
    margin=dict(t=36,b=24,l=8,r=8),
    legend=dict(orientation="h",yanchor="bottom",y=-0.28,xanchor="left",x=0),
    xaxis=dict(gridcolor="#E0E7E4",linecolor="#C0CEC9"),
    yaxis=dict(gridcolor="#E0E7E4",linecolor="#C0CEC9"),
)

def fig_(fig, h=300):
    fig.update_layout(height=h, **PLOT_LAYOUT)
    return fig

def fmt_money(v):
    if pd.isna(v): return "—"
    if v>=1_000_000: return f"${v/1_000_000:.1f}M"
    if v>=1_000:     return f"${v/1_000:.0f}K"
    return f"${v:,.0f}"

def kpi_html(label, value, meta="", badge=""):
    b = f"<span class=\'kpi-badge\'>{badge}</span>" if badge else ""
    return (f"<div class=\'kpi-card\'><div class=\'kpi-label\'>{label}</div>"
            f"<div class=\'kpi-value\'>{value}</div><div class=\'kpi-meta\'>{meta}{b}</div></div>")

def stat_strip_html(s):
    if s.empty: return "<div class=\'stat-strip\'><div class=\'stat-item\'><div class=\'stat-val\'>Sin datos</div></div></div>"
    items=[
        (f"{s.min():.0f}","Mín"),(f"{s.quantile(.10):.1f}","P10"),
        (f"{s.quantile(.25):.1f}","Q1"),(f"{s.median():.1f}","Mediana"),
        (f"{s.mean():.1f}","Promedio"),(f"{s.quantile(.75):.1f}","Q3"),
        (f"{s.quantile(.90):.1f}","P90"),(f"{s.max():.0f}","Máx"),
        (f"{s.std():.1f}","Desv. Est."),
    ]
    cells="".join(f"<div class=\'stat-item\'><div class=\'stat-val {\'green\' if l in (\'Mediana\',\'Promedio\') else \'\'} \'>{v}</div><div class=\'stat-lbl\'>{l}</div></div>" for v,l in items)
    return f"<div class=\'stat-strip\'>{cells}</div>"

LOGO_SVG = """
<svg width="26" height="26" viewBox="0 0 80 80" fill="none">
  <g transform="rotate(0,40,40)"><path d="M44 40 L44 14 Q44 6 36 6 L28 6 Q20 6 20 14 L20 22 Q20 28 28 28 L36 28 L36 40 Z" fill="#1AC77C"/></g>
  <g transform="rotate(120,40,40)"><path d="M44 40 L44 14 Q44 6 36 6 L28 6 Q20 6 20 14 L20 22 Q20 28 28 28 L36 28 L36 40 Z" fill="#1AC77C"/></g>
  <g transform="rotate(240,40,40)"><path d="M44 40 L44 14 Q44 6 36 6 L28 6 Q20 6 20 14 L20 22 Q20 28 28 28 L36 28 L36 40 Z" fill="#1AC77C"/></g>
</svg>"""

@st.cache_data(ttl=3600*22, show_spinner="Cargando datos desde Redshift…")
def load_data(force=False):
    return fetch_data(force_refresh=force)

def apply_filters(df, f):
    mask = pd.Series(True, index=df.index)
    if f.get("fecha_desde") and "fecha_lead" in df.columns:
        mask &= df["fecha_lead"].dt.date >= f["fecha_desde"]
    if f.get("fecha_hasta") and "fecha_lead" in df.columns:
        mask &= df["fecha_lead"].dt.date <= f["fecha_hasta"]
    if f.get("proyectos"):  mask &= df["proyecto_origen"].isin(f["proyectos"])
    if f.get("grupos"):     mask &= df["grupo_origen"].isin(f["grupos"])
    if f.get("tipos"):      mask &= df["tipo_auto"].isin(f["tipos"])
    if f.get("plazos"):     mask &= df["plazo_credito"].isin(f["plazos"])
    if f.get("marcas"):     mask &= df["marca_auto"].isin(f["marcas"])
    return df[mask].copy()

# ── Sidebar ───────────────────────────────────────────────────────────────────
def render_sidebar(df_raw):
    with st.sidebar:
        st.markdown(f"""<div style="display:flex;align-items:center;gap:10px;padding:8px 4px 20px">
          {LOGO_SVG}
          <div>
            <div style="font-family:\'Outfit\',sans-serif;font-size:1.25rem;font-weight:900;color:#F6F7F7;letter-spacing:-.03em">kuna</div>
            <div style="font-size:.55rem;text-transform:uppercase;letter-spacing:.15em;color:#485856">Intelligence</div>
          </div></div>""", unsafe_allow_html=True)

        page = st.radio("Nav", [
            "📊 Dashboard","⏱ Ciclo de Venta","📈 Tendencias",
            "🏢 Por Grupo","📋 Tabla de Datos",
            "─────────────",
            "⚗️ Correlaciones","📐 Gráficas","💾 Guardados"
        ], label_visibility="collapsed")

        st.markdown("---")
        st.markdown("<div style=\'font-size:.58rem;text-transform:uppercase;letter-spacing:.1em;color:#748C86;font-weight:700;margin-bottom:8px\'>Filtros globales</div>", unsafe_allow_html=True)

        min_d = df_raw["fecha_lead"].min().date() if df_raw["fecha_lead"].notna().any() else None
        max_d = df_raw["fecha_lead"].max().date() if df_raw["fecha_lead"].notna().any() else None
        fd = st.date_input("Desde", value=min_d, min_value=min_d, max_value=max_d)
        fh = st.date_input("Hasta", value=max_d, min_value=min_d, max_value=max_d)

        proyectos = sorted(df_raw["proyecto_origen"].dropna().unique().tolist())
        grupos    = sorted(df_raw["grupo_origen"].dropna().unique().tolist())
        tipos     = sorted(df_raw["tipo_auto"].dropna().unique().tolist())
        plazos    = sorted([int(p) for p in df_raw["plazo_credito"].dropna().unique().tolist()])
        marcas    = sorted(df_raw["marca_auto"].dropna().unique().tolist())

        sel_p = st.multiselect("Proyecto origen lead", proyectos, default=[])
        sel_g = st.multiselect("Grupo", grupos, default=[])
        sel_t = st.multiselect("Tipo de auto", tipos, default=[])
        sel_pl= st.multiselect("Plazo (meses)", plazos, default=[])
        sel_m = st.multiselect("Marca", marcas, default=[])

        st.markdown("---")
        if st.button("↻ Actualizar datos"):
            st.cache_data.clear(); st.rerun()
        st.caption(f"Última actualización: **{last_update()}**")

    return page, {"fecha_desde":fd,"fecha_hasta":fh,"proyectos":sel_p,
                  "grupos":sel_g,"tipos":sel_t,"plazos":sel_pl,"marcas":sel_m}

# ── Páginas ───────────────────────────────────────────────────────────────────
def page_dashboard(df):
    st.markdown("## Dashboard Ejecutivo")
    firmados = df[df["convirtio"]].copy()
    total, n_firm = len(df), len(firmados)
    ciclo = firmados["ciclo_dias_limpio"].dropna()

    c1,c2,c3,c4 = st.columns(4)
    with c1: st.markdown(kpi_html("Contratos Firmados", f"{n_firm:,}", f"de {total:,} leads", f"{n_firm/total*100:.1f}% conv." if total else ""), unsafe_allow_html=True)
    with c2: st.markdown(kpi_html("Ciclo Promedio", f"{ciclo.mean():.1f} días" if len(ciclo) else "—", f"mediana {ciclo.median():.1f} · σ={ciclo.std():.1f}" if len(ciclo) else ""), unsafe_allow_html=True)
    with c3: st.markdown(kpi_html("Monto Crédito Prom.", fmt_money(firmados["monto_credito"].mean()), f"plazo prom. {firmados[\'plazo_credito\'].mean():.0f}m" if n_firm else ""), unsafe_allow_html=True)
    with c4: st.markdown(kpi_html("Ingreso Mensual Prom.", fmt_money(firmados["ingresos"].mean()), f"ratio D/I: {firmados[\'ratio_deuda_ingreso\'].mean():.1f}×" if "ratio_deuda_ingreso" in firmados else ""), unsafe_allow_html=True)

    st.markdown("<div class=\'section-title\'>Distribución estadística del ciclo</div>", unsafe_allow_html=True)
    st.markdown(stat_strip_html(ciclo), unsafe_allow_html=True)

    col1,col2 = st.columns(2)
    with col1:
        fig=go.Figure(); fig.add_trace(go.Box(y=ciclo,name="Ciclo",marker_color=G,line_color=NAVY,boxmean=True,fillcolor="rgba(26,199,124,.15)"))
        fig.update_layout(title="Boxplot — Ciclo de Venta (días)",showlegend=False)
        st.plotly_chart(fig_(fig,300),use_container_width=True)
    with col2:
        fig=px.histogram(firmados.dropna(subset=["ciclo_dias_limpio"]),x="ciclo_dias_limpio",nbins=30,color_discrete_sequence=[G],title="Histograma del Ciclo",labels={"ciclo_dias_limpio":"Días"})
        fig.update_traces(marker_line_width=0)
        st.plotly_chart(fig_(fig,300),use_container_width=True)

    if "semana_firma" in firmados.columns and firmados["semana_firma"].notna().any():
        tend = firmados.groupby("semana_firma").agg(contratos=("contract_id","count"),ciclo_prom=("ciclo_dias_limpio","mean")).reset_index().sort_values("semana_firma")
        fig=make_subplots(specs=[[{"secondary_y":True}]])
        fig.add_trace(go.Bar(x=tend["semana_firma"],y=tend["contratos"],name="Contratos",marker_color="rgba(26,199,124,.5)"),secondary_y=False)
        fig.add_trace(go.Scatter(x=tend["semana_firma"],y=tend["ciclo_prom"].round(1),name="Ciclo prom.",line=dict(color=AMBER,width=2),mode="lines+markers"),secondary_y=True)
        fig.update_layout(title="Contratos y ciclo semanal",height=260,**PLOT_LAYOUT)
        fig.update_yaxes(title_text="Contratos",secondary_y=False,gridcolor="#E0E7E4")
        fig.update_yaxes(title_text="Días prom.",secondary_y=True,gridcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig,use_container_width=True)

    col3,col4 = st.columns(2)
    with col3:
        if "plazo_credito" in firmados and "tipo_auto" in firmados:
            mix=(firmados.dropna(subset=["plazo_credito","tipo_auto"]).groupby(["plazo_credito","tipo_auto"]).size().reset_index(name="n"))
            fig=px.bar(mix,x="n",y="plazo_credito",color="tipo_auto",orientation="h",color_discrete_sequence=PALETTE,title="Mix Plazo × Tipo",labels={"plazo_credito":"Plazo (m)","n":"Contratos","tipo_auto":"Tipo"})
            st.plotly_chart(fig_(fig,300),use_container_width=True)
    with col4:
        if "grupo_origen" in firmados:
            grp=(firmados.groupby("grupo_origen").agg(ciclo=("ciclo_dias_limpio","mean"),n=("contract_id","count")).reset_index().sort_values("ciclo"))
            fig=px.bar(grp,x="ciclo",y="grupo_origen",orientation="h",color="ciclo",color_continuous_scale=[[0,G],[0.5,G2],[1,NAVY]],title="Ciclo por Grupo",text=grp["ciclo"].round(1),labels={"ciclo":"Días prom.","grupo_origen":"Grupo"})
            fig.update_traces(textposition="outside"); fig.update_coloraxes(showscale=False)
            st.plotly_chart(fig_(fig,300),use_container_width=True)

def page_ciclo(df):
    st.markdown("## Ciclo de Venta")
    firmados=df[df["convirtio"]].copy()
    ciclo=firmados["ciclo_dias_limpio"].dropna()
    st.markdown(stat_strip_html(ciclo),unsafe_allow_html=True)
    col1,col2=st.columns(2)
    with col1:
        pd_=pd.DataFrame({"Percentil":["P10","Q1","Mediana","Q3","P90"],"Días":[ciclo.quantile(p) for p in [.10,.25,.50,.75,.90]]})
        fig=px.bar(pd_,x="Percentil",y="Días",color="Días",color_continuous_scale=[[0,G],[1,NAVY]],title="Percentiles del Ciclo",text=pd_["Días"].round(1))
        fig.update_traces(textposition="outside"); fig.update_coloraxes(showscale=False)
        st.plotly_chart(fig_(fig,300),use_container_width=True)
    with col2:
        if "tipo_auto" in firmados.columns and firmados["tipo_auto"].notna().any():
            fig=px.box(firmados.dropna(subset=["tipo_auto","ciclo_dias_limpio"]),x="tipo_auto",y="ciclo_dias_limpio",color="tipo_auto",color_discrete_sequence=PALETTE,title="Ciclo por Tipo de Auto",labels={"tipo_auto":"Tipo","ciclo_dias_limpio":"Días"})
            st.plotly_chart(fig_(fig,300),use_container_width=True)
    if "marca_auto" in firmados.columns:
        ms=(firmados.groupby("marca_auto")["ciclo_dias_limpio"].agg(["mean","median","count"]).reset_index().rename(columns={"mean":"Promedio","median":"Mediana","count":"n","marca_auto":"Marca"}).sort_values("Promedio").head(10))
        fig=go.Figure()
        fig.add_trace(go.Bar(x=ms["Marca"],y=ms["Promedio"],name="Promedio",marker_color="rgba(26,199,124,.5)"))
        fig.add_trace(go.Scatter(x=ms["Marca"],y=ms["Mediana"],name="Mediana",mode="markers",marker=dict(color=AMBER,size=9,symbol="diamond")))
        fig.update_layout(title="Ciclo por Marca (Top 10)")
        st.plotly_chart(fig_(fig,300),use_container_width=True)

def page_tendencias(df):
    st.markdown("## Tendencias")
    firmados=df[df["convirtio"]].copy()
    if "semana_firma" in firmados.columns:
        wk=(firmados.groupby("semana_firma").agg(contratos=("contract_id","count"),ciclo=("ciclo_dias_limpio","mean")).reset_index().sort_values("semana_firma"))
        fig=px.line(wk,x="semana_firma",y="ciclo",title="Ciclo promedio semanal",markers=True,color_discrete_sequence=[G],labels={"semana_firma":"Semana","ciclo":"Días"})
        fig.add_hline(y=wk["ciclo"].mean(),line_dash="dash",line_color=AMBER,annotation_text=f"Prom. {wk[\'ciclo\'].mean():.1f}d")
        st.plotly_chart(fig_(fig,280),use_container_width=True)
    col1,col2=st.columns(2)
    with col1:
        if "mes_firma" in firmados.columns:
            mom=(firmados.groupby("mes_firma").agg(contratos=("contract_id","count")).reset_index().sort_values("mes_firma"))
            mom["var_pct"]=mom["contratos"].pct_change()*100
            fig=go.Figure()
            fig.add_trace(go.Bar(x=mom["mes_firma"],y=mom["contratos"],name="Contratos",marker_color="rgba(26,199,124,.5)"))
            fig.add_trace(go.Scatter(x=mom["mes_firma"],y=mom["var_pct"],name="Var% MoM",yaxis="y2",line=dict(color=AMBER,width=2),mode="lines+markers"))
            fig.update_layout(title="Contratos MoM",height=280,**PLOT_LAYOUT,yaxis2=dict(overlaying="y",side="right",gridcolor="rgba(0,0,0,0)",title="Var %"))
            st.plotly_chart(fig,use_container_width=True)
    with col2:
        if "mes_firma" in firmados.columns and "tipo_auto" in firmados.columns:
            mix=(firmados.dropna(subset=["mes_firma","tipo_auto"]).groupby(["mes_firma","tipo_auto"]).size().reset_index(name="n").sort_values("mes_firma"))
            fig=px.bar(mix,x="mes_firma",y="n",color="tipo_auto",color_discrete_sequence=PALETTE,title="Mix Tipo por Mes",labels={"mes_firma":"Mes","n":"Contratos","tipo_auto":"Tipo"})
            st.plotly_chart(fig_(fig,280),use_container_width=True)

def page_grupos(df):
    st.markdown("## Por Grupo")
    firmados=df[df["convirtio"]].copy()
    if "grupo_origen" not in firmados.columns or firmados["grupo_origen"].isna().all():
        st.warning("Sin datos de grupo."); return
    gs=(firmados.groupby("grupo_origen").agg(contratos=("contract_id","count"),ciclo_prom=("ciclo_dias_limpio","mean"),monto_prom=("monto_credito","mean")).reset_index().sort_values("contratos",ascending=False))
    fig=make_subplots(specs=[[{"secondary_y":True}]])
    fig.add_trace(go.Bar(x=gs["grupo_origen"],y=gs["contratos"],name="Contratos",marker_color="rgba(26,199,124,.45)"),secondary_y=False)
    fig.add_trace(go.Scatter(x=gs["grupo_origen"],y=gs["ciclo_prom"].round(1),name="Ciclo prom.",line=dict(color=AMBER,width=2),mode="lines+markers+text",text=gs["ciclo_prom"].round(1),textposition="top center"),secondary_y=True)
    fig.update_layout(title="Contratos y Ciclo por Grupo",height=320,**PLOT_LAYOUT)
    fig.update_yaxes(title_text="Contratos",secondary_y=False,gridcolor="#E0E7E4")
    fig.update_yaxes(title_text="Días prom.",secondary_y=True,gridcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig,use_container_width=True)

    sel=st.selectbox("Drill-down — selecciona un grupo",sorted(firmados["grupo_origen"].dropna().unique().tolist()))
    if sel and "agencia_origen" in firmados.columns:
        ag=firmados[firmados["grupo_origen"]==sel]
        ags=(ag.groupby("agencia_origen").agg(contratos=("contract_id","count"),ciclo_prom=("ciclo_dias_limpio","mean")).reset_index().sort_values("ciclo_prom"))
        col1,col2=st.columns(2)
        with col1:
            fig=px.bar(ags,x="ciclo_prom",y="agencia_origen",orientation="h",title=f"Ciclo — {sel}",color="ciclo_prom",color_continuous_scale=[[0,G],[1,NAVY]],text=ags["ciclo_prom"].round(1),labels={"ciclo_prom":"Días","agencia_origen":"Agencia"})
            fig.update_coloraxes(showscale=False); st.plotly_chart(fig_(fig,320),use_container_width=True)
        with col2:
            fig=px.bar(ags,x="contratos",y="agencia_origen",orientation="h",title=f"Contratos — {sel}",color_discrete_sequence=["rgba(26,199,124,.5)"],text="contratos",labels={"contratos":"Contratos","agencia_origen":"Agencia"})
            st.plotly_chart(fig_(fig,320),use_container_width=True)

def page_tabla(df):
    st.markdown("## Tabla de Datos")
    all_cols=[c for c in df.columns]
    default=[c for c in ["fecha_lead","fecha_firma","ciclo_dias_limpio","convirtio","grupo_origen","agencia_origen","proyecto_origen","marca_auto","tipo_auto","anio_auto","monto_credito","plazo_credito","pago_inicial","ingresos","edad"] if c in all_cols]
    with st.expander("⚙️ Columnas visibles"):
        cols=st.multiselect("Columnas",all_cols,default=default)
    df_show=df[cols] if cols else df[default]
    st.dataframe(df_show.reset_index(drop=True),use_container_width=True,hide_index=True,height=480)
    csv=df_show.to_csv(index=False).encode("utf-8")
    st.download_button("⬇️ Exportar CSV",data=csv,file_name=f"kuna_{datetime.now().strftime(\'%Y%m%d\')}.csv",mime="text/csv")
    st.caption(f"{len(df_show):,} filas · {len(df_show.columns)} columnas")

def page_correlaciones(df):
    st.markdown("## ⚗️ Correlaciones")
    firmados=df[df["convirtio"]].copy()
    num=[c for c in firmados.select_dtypes(include=[np.number]).columns if firmados[c].notna().sum()>50 and c not in ["codigo_postal","anio_auto","convirtio"]]
    lbl={"ciclo_dias_limpio":"Ciclo (días)","monto_credito":"Monto ($)","plazo_credito":"Plazo (m)","edad":"Edad","ingresos":"Ingresos ($)","precio_auto":"Precio auto ($)","pago_inicial":"Pago inicial ($)","car_km":"Kilómetros","monto_mensualidad":"Mensualidad ($)","ratio_deuda_ingreso":"Ratio D/I","simu_tasa":"Tasa sim. (%)","simu_mf":"Monto fin. sim. ($)"}
    avail=[c for c in lbl if c in num]
    lbls=[lbl[c] for c in avail]
    l2c={v:k for k,v in lbl.items() if k in avail}
    c1,c2,c3,c4=st.columns([2,2,1,1])
    with c1: xl=st.selectbox("Variable X",lbls,index=0)
    with c2: yl=st.selectbox("Variable Y",lbls,index=1 if len(lbls)>1 else 0)
    with c3: gf=["Todos"]+sorted(df["grupo_origen"].dropna().unique().tolist()); sg=st.selectbox("Grupo",gf)
    with c4: tf=["Todos"]+sorted(df["tipo_auto"].dropna().unique().tolist()); st_=st.selectbox("Tipo",tf)
    xc=l2c.get(xl); yc=l2c.get(yl)
    sub=firmados.copy()
    if sg!="Todos": sub=sub[sub["grupo_origen"]==sg]
    if st_!="Todos": sub=sub[sub["tipo_auto"]==st_]
    sub=sub.dropna(subset=[xc,yc])
    if len(sub)<10: st.warning("Pocos datos. Amplía los filtros."); return
    xv=sub[xc].astype(float); yv=sub[yc].astype(float)
    slope,intercept=np.polyfit(xv,yv,1)
    yp=slope*xv+intercept
    r2=1-((yv-yp)**2).sum()/((yv-yv.mean())**2).sum()
    r=np.corrcoef(xv,yv)[0,1]
    show_t=st.toggle("Línea de tendencia OLS",value=True)
    left,right=st.columns([3,1])
    with left:
        fig=px.scatter(sub,x=xc,y=yc,opacity=.45,color_discrete_sequence=[G],labels={xc:xl,yc:yl},title=f"{xl} vs. {yl} — n={len(sub):,}")
        if show_t:
            xr=np.linspace(xv.min(),xv.max(),100)
            fig.add_trace(go.Scatter(x=xr,y=slope*xr+intercept,mode="lines",name="OLS",line=dict(color=AMBER,width=2)))
        st.plotly_chart(fig_(fig,380),use_container_width=True)
    with right:
        color=G if r2>=.5 else (AMBER if r2>=.25 else NEUTRAL)
        st.markdown(f"""<div style="background:white;border:1px solid #E0E7E4;border-radius:12px;padding:18px;text-align:center;margin-bottom:12px">
          <div style="font-size:.6rem;text-transform:uppercase;color:#748C86;font-family:Outfit,sans-serif;margin-bottom:6px">R²</div>
          <div style="font-family:Outfit,sans-serif;font-size:2rem;font-weight:900;color:{color}">{r2:.3f}</div>
          <div style="font-size:.7rem;color:#748C86">{"Fuerte" if r2>=.5 else "Moderada" if r2>=.25 else "Débil"}</div>
        </div>""",unsafe_allow_html=True)
        st.metric("r",f"{r:+.3f}"); st.metric("β",f"{slope:+.4f}"); st.metric("α",f"{intercept:,.1f}"); st.metric("n",f"{len(sub):,}")

def page_graficas(df):
    st.markdown("## 📐 Gráficas")
    firmados=df[df["convirtio"]].copy()
    num=firmados.select_dtypes(include=[np.number]).columns.tolist()
    cat=firmados.select_dtypes(include=["category","object"]).columns.tolist()
    TIPOS=["Barras verticales","Barras horizontales","Líneas","Dispersión","Boxplot","Apilado","Área"]
    with st.expander("⚙️ Configurar gráfica",expanded=True):
        c1,c2,c3=st.columns(3)
        with c1: ct=st.selectbox("Tipo",TIPOS)
        with c2: xc=st.selectbox("Eje X",cat+num,index=0)
        with c3: yc=st.selectbox("Eje Y",num,index=0)
        c4,c5,c6=st.columns(3)
        with c4: agg=st.selectbox("Agregación",["Promedio","Suma","Mediana","Conteo","Máx","Mín"])
        with c5: cc=st.selectbox("Agrupar (color)",["—"]+cat)
        with c6:
            show_ref=False
            if ct in ["Barras verticales","Barras horizontales"]:
                show_ref=st.toggle("Línea de referencia",False)
                if show_ref: rt=st.selectbox("Tipo ref.",["Promedio","Mediana","P25","P75"])
    ccl=None if cc=="—" else cc
    am={"Promedio":"mean","Suma":"sum","Mediana":"median","Conteo":"count","Máx":"max","Mín":"min"}
    try:
        if ccl: grp=firmados.dropna(subset=[xc]).groupby([xc,ccl])[yc].agg(am[agg]).reset_index()
        else:   grp=firmados.dropna(subset=[xc]).groupby(xc)[yc].agg(am[agg]).reset_index()
        grp=grp.sort_values(xc)
    except Exception as e: st.error(f"Error: {e}"); return
    yl=f"{agg} de {yc}"
    fig=None
    if ct=="Barras verticales":
        fig=px.bar(grp,x=xc,y=yc,color=ccl,color_discrete_sequence=PALETTE,title=f"{yl} por {xc}",labels={yc:yl})
        if show_ref:
            rv={"Promedio":grp[yc].mean(),"Mediana":grp[yc].median(),"P25":grp[yc].quantile(.25),"P75":grp[yc].quantile(.75)}[rt]
            fig.add_hline(y=rv,line_dash="dash",line_color=AMBER,annotation_text=f"{rt}: {rv:.1f}")
    elif ct=="Barras horizontales":
        fig=px.bar(grp,y=xc,x=yc,color=ccl,orientation="h",color_discrete_sequence=PALETTE,title=f"{yl} por {xc}",labels={yc:yl})
        if show_ref:
            rv={"Promedio":grp[yc].mean(),"Mediana":grp[yc].median(),"P25":grp[yc].quantile(.25),"P75":grp[yc].quantile(.75)}[rt]
            fig.add_vline(x=rv,line_dash="dash",line_color=AMBER,annotation_text=f"{rt}: {rv:.1f}")
    elif ct=="Líneas": fig=px.line(grp,x=xc,y=yc,color=ccl,color_discrete_sequence=PALETTE,markers=True,title=f"{yl} por {xc}")
    elif ct=="Dispersión": fig=px.scatter(firmados.dropna(subset=[xc,yc]),x=xc,y=yc,color=ccl,opacity=.5,color_discrete_sequence=PALETTE,title=f"{xc} vs. {yc}",trendline="ols" if not ccl else None)
    elif ct=="Boxplot": fig=px.box(firmados.dropna(subset=[xc,yc]),x=xc,y=yc,color=ccl,color_discrete_sequence=PALETTE,title=f"{yc} por {xc}")
    elif ct=="Apilado":
        if not ccl: st.warning("Apilado requiere variable de agrupación (color)."); return
        fig=px.bar(grp,x=xc,y=yc,color=ccl,barmode="stack",color_discrete_sequence=PALETTE,title=f"{yl} por {xc} apilado por {ccl}")
    elif ct=="Área": fig=px.area(grp,x=xc,y=yc,color=ccl,color_discrete_sequence=PALETTE,title=f"{yl} por {xc}")
    if fig:
        st.plotly_chart(fig_(fig,420),use_container_width=True)
        st.markdown("---")
        nc,bc=st.columns([3,1])
        with nc: nm=st.text_input("Nombre del análisis",placeholder="Ej: Ciclo por grupo Q1")
        with bc:
            st.markdown("<br>",unsafe_allow_html=True)
            if st.button("💾 Guardar"):
                if "saved" not in st.session_state: st.session_state["saved"]=[]
                st.session_state["saved"].append({"name":nm or f"Análisis {len(st.session_state[\'saved\'])+1}","type":ct,"x":xc,"y":yc,"agg":agg,"color":ccl,"date":datetime.now().strftime("%d/%m/%Y %H:%M")})
                st.success(f"✅ Guardado: {nm}")

def page_guardados():
    st.markdown("## 💾 Guardados")
    saved=st.session_state.get("saved",[])
    if not saved: st.info("Aún no hay análisis guardados. Ve a 📐 Gráficas y presiona Guardar."); return
    for i,s in enumerate(saved):
        col1,col2=st.columns([5,1])
        with col1: st.markdown(f"**{s[\'name\']}** · `{s[\'type\']}` · X:`{s[\'x\']}` Y:`{s[\'y\']}` Agg:`{s[\'agg\']}` · {s[\'date\']}")
        with col2:
            if st.button("🗑",key=f"d{i}"): st.session_state["saved"].pop(i); st.rerun()
        st.markdown("---")

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    try:
        df_raw=load_data()
    except Exception as e:
        st.error(f"❌ No se pudo conectar: {e}")
        st.info("Verifica .env y que estés en la VPN de Kavak.")
        st.stop()
    if df_raw.empty:
        st.warning("Dataset vacío. Revisa la query."); st.stop()

    page, filters = render_sidebar(df_raw)
    df = apply_filters(df_raw, filters)

    n_c=df["convirtio"].sum() if "convirtio" in df.columns else 0
    st.markdown(f"<div style=\'text-align:right;font-size:.7rem;color:#748C86;margin-bottom:-8px\'><b style=\'color:#1AC77C\'>{n_c:,}</b> contratos · <b>{len(df):,}</b> registros filtrados</div>",unsafe_allow_html=True)

    if   page=="📊 Dashboard":      page_dashboard(df)
    elif page=="⏱ Ciclo de Venta":  page_ciclo(df)
    elif page=="📈 Tendencias":      page_tendencias(df)
    elif page=="🏢 Por Grupo":       page_grupos(df)
    elif page=="📋 Tabla de Datos":  page_tabla(df)
    elif page=="⚗️ Correlaciones":   page_correlaciones(df)
    elif page=="📐 Gráficas":        page_graficas(df)
    elif page=="💾 Guardados":       page_guardados()

if __name__=="__main__":
    main()
'''

# ══════════════════════════════════════════════════════════════════════════════
# EJECUTAR LA CREACIÓN
# ══════════════════════════════════════════════════════════════════════════════
def main():
    print(f"\n{'='*54}")
    print("  Kuna Intelligence — Instalador")
    print(f"{'='*54}\n")
    print(f"  Destino: {BASE}\n")

    # Crear directorios
    for d in ["data", "data/cache", "metrics", "tests", "logs"]:
        (BASE / d).mkdir(parents=True, exist_ok=True)
        ok(f"Carpeta: {d}/")

    # Escribir archivos
    print()
    for rel_path, content in FILES.items():
        full = BASE / rel_path
        full.parent.mkdir(parents=True, exist_ok=True)
        full.write_text(content, encoding="utf-8")
        ok(f"Creado: {rel_path}")

    print(f"\n{'='*54}")
    print(f"  ✅  {len(FILES)} archivos creados en:")
    print(f"  {BASE}")
    print(f"{'='*54}")
    print("""
  Próximos pasos:
  ─────────────────────────────────────────────────────
  1. Abre una terminal en esa carpeta:
     cd "C:\\Users\\sahe.careaga_kavak\\Documents\\Análisis\\
         Análisis_Python\\Claude\\Intranet_Analysis"

  2. Crea el entorno virtual e instala:
     python -m venv venv
     venv\\Scripts\\activate
     pip install -r requirements.txt

  3. Verifica la conexión a Redshift (necesitas VPN):
     python check_connection.py

  4. Arranca la app:
     streamlit run app.py

  → Abre http://localhost:8501
  ─────────────────────────────────────────────────────
""")


if __name__ == "__main__":
    main()