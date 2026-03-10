"""
refresh_table.py — Materializa la query maestra en playground.kuna_intranet_nrfm

Uso:
    python refresh_table.py           # reconstruye la tabla
    python refresh_table.py --dry-run # solo valida conexión y cuenta filas sin escribir
"""
import os, sys, time, logging, argparse
from pathlib import Path
from datetime import datetime
import psycopg2
from dotenv import load_dotenv

Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/kuna.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("kuna.refresh")

load_dotenv()

TARGET_SCHEMA = "playground"
TARGET_TABLE  = "kua_intelligence_v1"
FULL_TABLE    = f'"{TARGET_SCHEMA}"."{TARGET_TABLE}"'

# ── Query de negocio ──────────────────────────────────────────────────────────
# QUALIFY reemplazado por subconsulta (compatibilidad Redshift antiguo)
QUERY_BUILD = f"""
CREATE TABLE {FULL_TABLE} AS
WITH leads AS (
    SELECT
        convert_timezone('America/Mexico_City', create_date) AS fecha_lead,
        financing_application_id,
        user_id,
        vehicle_id AS asset,
        ksan.grupo                   AS grupo_origen,
        ksan.nombre_completo_agencia AS agencia_origen,
        ksan.proyecto_f              AS proyecto_origen
    FROM financing_acceptation_api_global_refined.financing_leads_data fld
    LEFT JOIN playground.kua_sensitive_ao_nexus ksan ON fld.agency_id = ksan.agency_id
    WHERE country_id = '484'
      AND convert_timezone('America/Mexico_City', create_date) >= '2025-09-01'
),
contratos AS (
    SELECT
        convert_timezone('America/Mexico_City', m."date") AS fecha_firma,
        m.contract_id,
        json_extract_path_text(m."data",'client','uuid')                              AS user_id,
        json_extract_path_text(m."data",'car','make')                                 AS marca_auto,
        CAST(json_extract_path_text(m."data",'car','value')      AS DECIMAL(12,2))    AS precio_auto,
        CAST(json_extract_path_text(m."data",'car','year')       AS INTEGER)          AS anio_auto,
        json_extract_path_text(m."data",'car','model')                                AS modelo_auto,
        json_extract_path_text(m."data",'car','version')                              AS version_auto,
        CAST(json_extract_path_text(m."data",'car','km')         AS NUMERIC)          AS car_km,
        json_extract_path_text(m."data",'car','condition')                            AS tipo_auto,
        CAST(json_extract_path_text(m."data",'car','interest')   AS DECIMAL(10,2))    AS interes_coche,
        json_extract_path_text(m."data",'insurance','name')                           AS proveedor_seguro,
        CAST(json_extract_path_text(m."data",'insurance','price') AS DECIMAL(10,2))   AS precio_seguro,
        json_extract_path_text(m."data",'insurance','insurancePaymentMethod')         AS metodo_pago_seguro,
        CAST(json_extract_path_text(m."data",'credit','amount')  AS DECIMAL(12,2))    AS monto_credito,
        CAST(CAST(json_extract_path_text(m."data",'car','financingAmount') AS DECIMAL(12,0)) AS INTEGER) AS monto_financiar,
        CAST(json_extract_path_text(m."data",'credit','monthlyPayment') AS DECIMAL(10,2))    AS monto_mensualidad,
        CAST(CAST(json_extract_path_text(m."data",'credit','installments') AS DECIMAL(10,0)) AS INTEGER) AS plazo_credito,
        CAST(json_extract_path_text(m."data",'credit','initialPayment')  AS DECIMAL(12,2))   AS pago_inicial,
        CAST(json_extract_path_text(m."data",'client','profiling','income') AS DECIMAL(12,2)) AS ingresos,
        CAST(CAST(json_extract_path_text(m."data",'client','address','zipCode') AS DECIMAL(10,0)) AS INTEGER) AS codigo_postal,
        DATEDIFF(year, CAST(json_extract_path_text(m."data",'client','birthday') AS DATE), CURRENT_DATE) AS edad,
        json_extract_path_text(m."data",'client','bank','bank')                      AS banco_cliente,
        CAST(CAST(json_extract_path_text(m."data",'car','stockId') AS DECIMAL(10,0)) AS INTEGER) AS asset,
        json_extract_path_text(m."data",'credit','branch','deal')                    AS proyecto,
        json_extract_path_text(m."data",'credit','branch','agencyId')                AS agency_id
    FROM financing_contract_api_global_refined.metrics AS m
    WHERE upper(m.status) = 'CONTRATO FIRMADO'
      AND json_extract_path_text(m."data",'credit','branch','deal') = 'NRFM'
      AND convert_timezone('America/Mexico_City', m."date") >= '2025-09-01'::DATE
),
base_simus AS (
    SELECT
        CONVERT_TIMEZONE('America/Mexico_City', cso.created_at) AS fecha_creacion_simulacion,
        cso.user_id, s.offer_id,
        CAST(cso.installments::numeric AS INT) AS plazo,
        ROUND(cso.installment_amount, 2)        AS mensualidad
    FROM financing_products_api_global_refined.ca_simulation_option cso
    JOIN financing_products_api_global_refined.fpm_simulation s ON cso.simulation_id = s.id
    JOIN financing_products_api_global_refined.fpm_simulation_request fsr ON fsr.simulation_id = s.id
    WHERE cso.status = 'CONFIRMED'
),
simus_enriquecidas AS (
    SELECT
        b.fecha_creacion_simulacion, b.user_id, b.plazo, b.mensualidad,
        ffr.annual_rate AS tasa, ffr.financed_amount AS mf,
        ROUND(json_extract_path_text(ffr.details,'initialPeriod','car','balance')::NUMERIC,2) AS precio_auto_simulacion,
        ROUND(json_extract_path_text(ffr.details,'initialPeriod','car','capital')::NUMERIC,2) AS enganche_simulacion,
        ROUND(json_extract_path_text(ffr.details,'initialPeriod','total')::NUMERIC,2)          AS pago_inicial_simulacion,
        json_extract_path_text(ffr.details,'downPaymentRate')                                  AS enganche_pct_simulacion,
        TRY_CAST(ffr.vehicle_id AS INTEGER) AS asset
    FROM base_simus b
    LEFT JOIN financing_products_api_global_refined.fpm_financial_run ffr ON ffr.offer_id = b.offer_id
    WHERE b.fecha_creacion_simulacion >= '2025-09-01'
),
ultima_simu AS (
    SELECT * FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY user_id, asset ORDER BY fecha_creacion_simulacion DESC) AS _rn
        FROM simus_enriquecidas
    ) WHERE _rn = 1
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
"""

QUERY_COUNT  = f"SELECT COUNT(*) FROM {FULL_TABLE}"
QUERY_DROP   = f"DROP TABLE IF EXISTS {FULL_TABLE}"


def _connect():
    return psycopg2.connect(
        host=os.getenv("REDSHIFT_HOST"),
        port=int(os.getenv("REDSHIFT_PORT", 5439)),
        dbname=os.getenv("REDSHIFT_DATABASE"),
        user=os.getenv("REDSHIFT_USER"),
        password=os.getenv("REDSHIFT_PASSWORD"),
        connect_timeout=int(os.getenv("QUERY_TIMEOUT_SECONDS", 300)),
    )


def refresh(dry_run=False):
    logger.info("=== refresh_table.py — %s ===", datetime.now().strftime("%Y-%m-%d %H:%M"))
    conn = _connect()
    conn.autocommit = True          # DDL en Redshift requiere autocommit
    cur = conn.cursor()

    if dry_run:
        logger.info("DRY-RUN: verificando conteo en tablas fuente...")
        cur.execute("""
            SELECT COUNT(*) FROM financing_acceptation_api_global_refined.financing_leads_data
            WHERE country_id='484'
              AND convert_timezone('America/Mexico_City',create_date)>='2025-09-01'
        """)
        logger.info("Leads fuente: %s", cur.fetchone()[0])
        cur.execute("""
            SELECT COUNT(*) FROM financing_contract_api_global_refined.metrics
            WHERE upper(status)='CONTRATO FIRMADO'
              AND json_extract_path_text("data",'credit','branch','deal')='NRFM'
              AND convert_timezone('America/Mexico_City',"date")>='2025-09-01'::DATE
        """)
        logger.info("Contratos NRFM fuente: %s", cur.fetchone()[0])
        cur.close(); conn.close()
        return

    # 1. DROP tabla anterior
    logger.info("Eliminando tabla anterior (si existe)...")
    cur.execute(QUERY_DROP)

    # 2. CREATE TABLE AS SELECT
    logger.info("Construyendo %s ...", FULL_TABLE)
    t0 = time.time()
    cur.execute(QUERY_BUILD)
    elapsed = time.time() - t0
    logger.info("Tabla creada en %.1fs", elapsed)

    # 3. Contar filas
    cur.execute(QUERY_COUNT)
    n = cur.fetchone()[0]
    logger.info("Filas en %s: %s", FULL_TABLE, f"{n:,}")

    cur.close()
    conn.close()
    logger.info("=== Listo — actualiza el cache de la app con: python -c \"from data.connector import fetch_data; fetch_data(force_refresh=True)\" ===")
    return n


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Solo verifica fuentes sin escribir")
    args = parser.parse_args()
    try:
        refresh(dry_run=args.dry_run)
    except Exception as e:
        logger.error("FALLO: %s", e)
        sys.exit(1)
