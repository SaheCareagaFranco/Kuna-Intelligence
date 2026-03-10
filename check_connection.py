"""
check_connection.py — Verifica conexion a Redshift antes de arrancar la app
Uso: python check_connection.py
"""
import os, sys, socket, time
from dotenv import load_dotenv

load_dotenv()
G="[92m"; R="[91m"; Y="[93m"; B="[94m"; E="[0m"; BD="[1m"
ok   = lambda m: print(f"  {G}OK{E}  {m}")
fail = lambda m: print(f"  {R}XX{E}  {m}")
warn = lambda m: print(f"  {Y}!!{E}  {m}")
info = lambda m: print(f"  {B}->{E}  {m}")
errors = []

print(f"\n{BD}{'='*54}{E}")
print(f"{BD}  Kuna Intelligence — Verificacion de conexion{E}")
print(f"{BD}{'='*54}{E}")

# 1. Variables de entorno
print(f"\n{BD}[1] Variables de entorno{E}")
VARS = {k: os.getenv(k) for k in ["REDSHIFT_HOST","REDSHIFT_PORT","REDSHIFT_DATABASE","REDSHIFT_USER","REDSHIFT_PASSWORD"]}
for k,v in VARS.items():
    if v: ok(f"{k} = {'***' if 'PASSWORD' in k else v}")
    else: fail(f"{k} no definida"); errors.append(k)
if errors: print(f"\n{R}Faltan variables.{E}"); sys.exit(1)

HOST=VARS["REDSHIFT_HOST"]; PORT=int(VARS.get("REDSHIFT_PORT",5439))
DB=VARS["REDSHIFT_DATABASE"]; USER=VARS["REDSHIFT_USER"]; PWD=VARS["REDSHIFT_PASSWORD"]

# 2. TCP
print(f"\n{BD}[2] Conectividad TCP{E}")
try:
    t0=time.time()
    s=socket.create_connection((HOST,PORT),timeout=10); s.close()
    ok(f"Puerto {PORT} alcanzable en {(time.time()-t0)*1000:.0f}ms")
except Exception as e:
    fail(f"No se puede conectar a {HOST}:{PORT} — {e}")
    warn("Verifica que estes conectado a la VPN de Kavak")
    errors.append("TCP"); print(f"\n{R}Sin conectividad.{E}"); sys.exit(1)

# 3. Auth  (psycopg2 directo — SQLAlchemy 2.x no es compatible con Redshift)
print(f"\n{BD}[3] Autenticacion{E}")
try:
    import psycopg2
    _conn = psycopg2.connect(host=HOST, port=PORT, dbname=DB, user=USER, password=PWD, connect_timeout=30)
    _cur = _conn.cursor()
    _cur.execute("SELECT current_user, current_database()")
    row = _cur.fetchone()
    _cur.close()
    ok(f"Login como: {row[0]} en BD: {row[1]}")
except Exception as e:
    fail(f"Error de autenticacion: {e}"); errors.append("AUTH")
    print(f"\n{R}No se pudo autenticar.{E}"); sys.exit(1)

# 4. Tablas
print(f"\n{BD}[4] Acceso a tablas{E}")
TABLES=[
    ("financing_acceptation_api_global_refined","financing_leads_data"),
    ("financing_contract_api_global_refined","metrics"),
    ("financing_products_api_global_refined","ca_simulation_option"),
    ("financing_products_api_global_refined","fpm_simulation"),
    ("financing_products_api_global_refined","fpm_financial_run"),
    ("playground","kua_sensitive_ao_nexus"),
]
for schema,table in TABLES:
    try:
        _cur = _conn.cursor()
        _cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
        cnt = _cur.fetchone()[0]
        _cur.close()
        ok(f"{schema}.{table}  ({cnt:,} filas)")
    except Exception as e:
        _conn.rollback()
        fail(f"{schema}.{table}  — {str(e)[:70]}")
        errors.append(f"{schema}.{table}")

# 5. Query COUNT
print(f"\n{BD}[5] Validacion de query maestra{E}")
try:
    t0=time.time()
    _cur = _conn.cursor()
    _cur.execute("""
        SELECT
            (SELECT COUNT(*) FROM financing_acceptation_api_global_refined.financing_leads_data
             WHERE country_id='484' AND convert_timezone('America/Mexico_City',create_date)>='2025-09-01') AS leads,
            (SELECT COUNT(*) FROM financing_contract_api_global_refined.metrics
             WHERE upper(status)='CONTRATO FIRMADO'
               AND json_extract_path_text("data",'credit','branch','deal')='NRFM'
               AND convert_timezone('America/Mexico_City',"date")>='2025-09-01'::DATE) AS contratos
    """)
    row = _cur.fetchone()
    _cur.close()
    _conn.close()
    ok(f"Query ejecutada en {time.time()-t0:.1f}s")
    ok(f"Leads encontrados:      {row[0]:,}")
    ok(f"Contratos NRFM:         {row[1]:,}")
    if row[0]: info(f"Conversion estimada:    {row[1]/row[0]*100:.1f}%")
except Exception as e:
    fail(f"Error en query: {e}"); errors.append("QUERY")

# Resultado final
print(f"\n{BD}{'='*54}{E}")
if not errors:
    print(f"{G}{BD}  TODO OK — Arranca la app con:{E}")
    print(f"\n  {BD}streamlit run app.py{E}\n")
else:
    print(f"{R}{BD}  {len(errors)} problema(s) encontrados.{E}")
    for e in errors: print(f"  - {e}")
print(f"{BD}{'='*54}{E}\n")
