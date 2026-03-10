# Databricks notebook source
# MAGIC %md
# MAGIC # Bakehouse Cookie Co. — Full Environment Setup
# MAGIC
# MAGIC This notebook provisions all infrastructure from scratch:
# MAGIC 1. Creates a SQL Warehouse (if none exists)
# MAGIC 2. Creates a Lakebase project + waits for it to be ready
# MAGIC 3. Creates operational tables and seeds data from Delta Share
# MAGIC 4. Creates the `gold_live` hybrid table
# MAGIC 5. Configures roles and permissions for the app service principal
# MAGIC 6. Creates a foreign catalog for the SDP pipeline to read from Lakebase

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "lakebase_demo")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
PROJECT_NAME = "bakehouse-lakebase"
PGDATABASE = "databricks_postgres"

# COMMAND ----------

import time
import json
import requests
import psycopg
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
host = w.config.host
user = w.current_user.me().user_name

def api_headers():
    return {**w.config._header_factory(), "Content-Type": "application/json"}

if not CATALOG:
    catalogs = [c for c in w.catalogs.list() if c.catalog_type and "MANAGED" in str(c.catalog_type)]
    if catalogs:
        CATALOG = catalogs[0].name
        print(f"Auto-detected catalog: {CATALOG}")
    else:
        raise RuntimeError("No managed catalog found. Please set the 'catalog' parameter.")

print(f"User: {user}")
print(f"Catalog: {CATALOG}")
print(f"Schema: {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create SQL Warehouse

# COMMAND ----------

warehouses = list(w.warehouses.list())
wh = None
for wh_candidate in warehouses:
    if wh_candidate.state and wh_candidate.state.value in ("RUNNING", "STARTING", "STOPPED"):
        wh = wh_candidate
        break

if wh:
    WH_ID = wh.id
    print(f"Using existing warehouse: {wh.name} ({WH_ID})")
else:
    print("No warehouse found. Creating one...")
    resp = requests.post(f"{host}/api/2.0/sql/warehouses", headers=api_headers(), json={
        "name": "Bakehouse Analytics",
        "cluster_size": "2X-Small",
        "max_num_clusters": 1,
        "auto_stop_mins": 10,
        "warehouse_type": "PRO",
        "enable_serverless_compute": True,
    })
    result = resp.json()
    WH_ID = result.get("id")
    print(f"Created warehouse: {WH_ID}")

    for _ in range(30):
        status = requests.get(f"{host}/api/2.0/sql/warehouses/{WH_ID}", headers=api_headers()).json()
        state = status.get("state", "UNKNOWN")
        if state == "RUNNING":
            print("Warehouse is running")
            break
        print(f"  Warehouse state: {state}...")
        time.sleep(10)

print(f"Warehouse ID: {WH_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Lakebase Project

# COMMAND ----------

projects_resp = requests.get(f"{host}/api/2.0/postgres/projects", headers=api_headers())
projects = projects_resp.json().get("projects", [])
project = None
for p in projects:
    if PROJECT_NAME in p.get("name", ""):
        project = p
        break

if project:
    print(f"Lakebase project already exists: {project['name']}")
else:
    print(f"Creating Lakebase project: {PROJECT_NAME}...")
    resp = requests.post(f"{host}/api/2.0/postgres/projects?project_id={PROJECT_NAME}", headers=api_headers(), json={
        "spec": {
            "display_name": PROJECT_NAME,
            "pg_version": "17",
            "enable_pg_native_login": True,
        }
    })
    if resp.status_code in (200, 201):
        print("Project creation initiated")
    else:
        print(f"Project creation response: {resp.status_code} {resp.text[:300]}")

    for _ in range(30):
        time.sleep(10)
        projects_resp = requests.get(f"{host}/api/2.0/postgres/projects", headers=api_headers())
        for p in projects_resp.json().get("projects", []):
            if PROJECT_NAME in p.get("name", ""):
                project = p
                break
        if project:
            status = project.get("status", {})
            is_ready = status.get("compute_last_active_time") or status.get("current_state") == "READY"
            if is_ready:
                print("Lakebase project is active")
                break
            print(f"  Waiting for project to become active (state: {status.get('current_state', 'unknown')})...")

if not project:
    raise RuntimeError("Failed to create Lakebase project")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Discover Lakebase Connection Details

# COMMAND ----------

raw_name = project["name"]
full_project_name = raw_name if raw_name.startswith("projects/") else f"projects/{raw_name}"

branches_resp = requests.get(f"{host}/api/2.0/postgres/{full_project_name}/branches", headers=api_headers())
branches = branches_resp.json().get("branches", [])
branch = branches[0] if branches else None
if not branch:
    raise RuntimeError("No branch found for the Lakebase project")

branch_name = branch["name"]
endpoints_resp = requests.get(f"{host}/api/2.0/postgres/{branch_name}/endpoints", headers=api_headers())
endpoints = endpoints_resp.json().get("endpoints", [])
endpoint = endpoints[0] if endpoints else None
if not endpoint:
    raise RuntimeError("No endpoint found")

PGHOST = endpoint["status"]["hosts"]["host"]
ENDPOINT_NAME = endpoint["name"]

print(f"PGHOST: {PGHOST}")
print(f"ENDPOINT_NAME: {ENDPOINT_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Connect to Lakebase and Create Tables

# COMMAND ----------

cred = w.postgres.generate_database_credential(endpoint=ENDPOINT_NAME)
conn = psycopg.connect(
    host=PGHOST, port=5432, dbname=PGDATABASE,
    user=user, password=cred.token, sslmode="require"
)
cur = conn.cursor()
print(f"Connected to Lakebase as {user}")

# COMMAND ----------

cur.execute("DROP TABLE IF EXISTS transactions")
cur.execute("DROP TABLE IF EXISTS retention_offers")
cur.execute("DROP TABLE IF EXISTS gold_live")
cur.execute("DROP TABLE IF EXISTS customers")
cur.execute("DROP TABLE IF EXISTS franchises")
cur.execute("DROP TABLE IF EXISTS supplies")
conn.commit()

cur.execute("""
    CREATE TABLE customers (
        customerid BIGINT PRIMARY KEY,
        email_address VARCHAR(255),
        first_name VARCHAR(100),
        last_name VARCHAR(100),
        full_name VARCHAR(200),
        phone_number VARCHAR(50),
        address VARCHAR(500),
        city VARCHAR(100),
        state VARCHAR(100),
        country VARCHAR(100),
        continent VARCHAR(100),
        postal_zip_code BIGINT,
        gender VARCHAR(20)
    )
""")
cur.execute("""
    CREATE TABLE franchises (
        franchiseid BIGINT PRIMARY KEY,
        franchise_name VARCHAR(200),
        city VARCHAR(100),
        district VARCHAR(100),
        zipcode VARCHAR(20),
        country VARCHAR(100),
        size VARCHAR(50),
        longitude DOUBLE PRECISION,
        latitude DOUBLE PRECISION,
        supplierid BIGINT
    )
""")
cur.execute("""
    CREATE TABLE supplies (
        supplierid BIGINT PRIMARY KEY,
        supplier_name VARCHAR(200),
        ingredient VARCHAR(200),
        continent VARCHAR(100),
        city VARCHAR(100),
        district VARCHAR(100),
        size VARCHAR(50),
        longitude DOUBLE PRECISION,
        latitude DOUBLE PRECISION,
        approved VARCHAR(20)
    )
""")
cur.execute("""
    CREATE TABLE transactions (
        transactionid BIGINT PRIMARY KEY,
        customerid BIGINT,
        franchiseid BIGINT,
        datetime TIMESTAMP,
        transaction_date DATE,
        product VARCHAR(200),
        quantity BIGINT,
        unitprice BIGINT,
        totalprice BIGINT,
        payment_method VARCHAR(50),
        cardnumber BIGINT,
        transaction_year INTEGER,
        transaction_month INTEGER,
        day_of_week INTEGER,
        hour_of_day INTEGER
    )
""")
cur.execute("""
    CREATE TABLE retention_offers (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        customerid BIGINT,
        offer_type VARCHAR(100),
        offer_detail VARCHAR(500),
        status VARCHAR(50),
        created_at TIMESTAMP
    )
""")
cur.execute("""
    CREATE TABLE gold_live (
        "customerID" BIGINT PRIMARY KEY,
        full_name VARCHAR(200),
        first_name VARCHAR(100),
        last_name VARCHAR(100),
        email_address VARCHAR(255),
        phone_number VARCHAR(50),
        city VARCHAR(100),
        state VARCHAR(100),
        country VARCHAR(100),
        continent VARCHAR(100),
        gender VARCHAR(20),
        total_transactions BIGINT,
        total_spent BIGINT,
        avg_order_value DOUBLE PRECISION,
        days_since_last_purchase INTEGER,
        unique_franchises_visited BIGINT,
        total_items_purchased BIGINT,
        customer_value_segment VARCHAR(50),
        customer_connection_segment VARCHAR(50),
        first_purchase_date TIMESTAMP,
        last_purchase_date TIMESTAMP,
        churn_probability DOUBLE PRECISION,
        updated_at TIMESTAMP,
        source VARCHAR(20)
    )
""")
conn.commit()
print("All tables created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Seed Data from Delta Share

# COMMAND ----------

def query_to_rows(sql):
    resp = w.statement_execution.execute_statement(
        warehouse_id=WH_ID, statement=sql, wait_timeout="50s"
    )
    while resp.status and resp.status.state and resp.status.state.value in ("PENDING", "RUNNING"):
        time.sleep(3)
        resp = w.statement_execution.get_statement(resp.statement_id)
    if resp.status and resp.status.state and resp.status.state.value == "FAILED":
        raise RuntimeError(f"Query failed: {resp.status.error}")
    return resp.result.data_array if resp.result and resp.result.data_array else []

# COMMAND ----------

rows = query_to_rows("""
    SELECT customerID, TRIM(LOWER(email_address)), INITCAP(TRIM(first_name)), INITCAP(TRIM(last_name)),
           CONCAT(INITCAP(TRIM(first_name)), ' ', INITCAP(TRIM(last_name))),
           phone_number, TRIM(address), TRIM(city), TRIM(state), TRIM(country), TRIM(continent),
           postal_zip_code, gender
    FROM samples.bakehouse.sales_customers WHERE customerID IS NOT NULL
""")
print(f"Seeding {len(rows)} customers...")
ph = ",".join(["%s"] * 13)
for row in rows:
    cur.execute(f"INSERT INTO customers VALUES ({ph})", [None if v is None else v for v in row])
conn.commit()

# COMMAND ----------

rows = query_to_rows("""
    SELECT franchiseID, INITCAP(TRIM(name)), TRIM(city), TRIM(district), TRIM(zipcode),
           TRIM(country), TRIM(size), longitude, latitude, supplierID
    FROM samples.bakehouse.sales_franchises WHERE franchiseID IS NOT NULL
""")
print(f"Seeding {len(rows)} franchises...")
ph = ",".join(["%s"] * 10)
for row in rows:
    cur.execute(f"INSERT INTO franchises VALUES ({ph})", [None if v is None else v for v in row])
conn.commit()

# COMMAND ----------

rows = query_to_rows("""
    SELECT supplierID, TRIM(name), TRIM(ingredient), TRIM(continent), TRIM(city),
           TRIM(district), TRIM(size), longitude, latitude, TRIM(LOWER(approved))
    FROM samples.bakehouse.sales_suppliers WHERE supplierID IS NOT NULL
""")
print(f"Seeding {len(rows)} supplies...")
ph = ",".join(["%s"] * 10)
for row in rows:
    cur.execute(f"INSERT INTO supplies VALUES ({ph})", [None if v is None else v for v in row])
conn.commit()

# COMMAND ----------

rows = query_to_rows("""
    SELECT transactionID, customerID, franchiseID, dateTime, DATE(dateTime),
           TRIM(product), quantity, unitPrice, totalPrice, TRIM(paymentMethod), cardNumber,
           YEAR(dateTime), MONTH(dateTime), DAYOFWEEK(dateTime), HOUR(dateTime)
    FROM samples.bakehouse.sales_transactions
    WHERE transactionID IS NOT NULL AND totalPrice > 0
""")
print(f"Seeding {len(rows)} transactions...")
ph = ",".join(["%s"] * 15)
for row in rows:
    cur.execute(f"INSERT INTO transactions VALUES ({ph})", [None if v is None else v for v in row])
conn.commit()
print("All data seeded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Grant Permissions to App Service Principal

# COMMAND ----------

apps_resp = requests.get(f"{host}/api/2.0/apps", headers=api_headers())
apps = apps_resp.json().get("apps", [])
sp_id = None
for a in apps:
    if "bakehouse" in a.get("name", "").lower() or "lakebase" in a.get("name", "").lower() or "c360" in a.get("name", "").lower():
        sp_id = a.get("service_principal_client_id")
        print(f"Found app: {a['name']} with SP: {sp_id}")
        break

if not sp_id:
    print("WARNING: No app found yet. Permissions will need to be granted after the app is created.")
else:
    try:
        cur.execute("CREATE EXTENSION IF NOT EXISTS databricks_auth")
        conn.commit()
    except Exception:
        conn.rollback()

    try:
        cur.execute(f"SELECT databricks_create_role('{sp_id}', 'service_principal')")
        conn.commit()
        print(f"Created OAuth role for {sp_id}")
    except Exception:
        conn.rollback()
        print(f"OAuth role may already exist for {sp_id}")

    for g in [
        f'GRANT CONNECT ON DATABASE {PGDATABASE} TO "{sp_id}"',
        f'GRANT USAGE ON SCHEMA public TO "{sp_id}"',
        f'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO "{sp_id}"',
    ]:
        try:
            cur.execute(g)
            conn.commit()
            print(f"  OK: {g[:60]}...")
        except Exception as e:
            conn.rollback()
            print(f"  SKIP: {e}")

    # lakebase_demo schema may not exist yet (created after pipeline + synced table)
    for g in [
        f'GRANT USAGE ON SCHEMA lakebase_demo TO "{sp_id}"',
        f'GRANT SELECT ON ALL TABLES IN SCHEMA lakebase_demo TO "{sp_id}"',
    ]:
        try:
            cur.execute(g)
            conn.commit()
            print(f"  OK: {g[:60]}...")
        except Exception:
            conn.rollback()
            print(f"  SKIP (schema may not exist yet, re-run after creating synced table)")

    for stmt in [
        f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `{sp_id}`",
        f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{SCHEMA} TO `{sp_id}`",
        f"GRANT SELECT ON SCHEMA {CATALOG}.{SCHEMA} TO `{sp_id}`",
    ]:
        try:
            query_to_rows(stmt)
        except Exception:
            pass

    requests.patch(
        f"{host}/api/2.0/permissions/warehouses/{WH_ID}",
        headers=api_headers(),
        json={"access_control_list": [{"service_principal_name": sp_id, "permission_level": "CAN_USE"}]}
    )
    print("All permissions granted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Seed gold_live from Synced Table (if available)

# COMMAND ----------

try:
    cur.execute("""
        INSERT INTO gold_live
        SELECT g.*, NOW() as updated_at, 'synced' as source
        FROM lakebase_demo.gold_insights g
    """)
    conn.commit()
    print("gold_live seeded from synced table")
except Exception as e:
    print(f"gold_live seed skipped (synced table not available yet, will populate after pipeline runs): {e}")
    conn.rollback()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Output Configuration
# MAGIC
# MAGIC These values are needed for the app.yaml and other notebooks.

# COMMAND ----------

conn.close()

output = {
    "catalog": CATALOG,
    "schema": SCHEMA,
    "warehouse_id": WH_ID,
    "pghost": PGHOST,
    "pgdatabase": PGDATABASE,
    "endpoint_name": ENDPOINT_NAME,
    "service_principal_id": sp_id or "unknown",
}

print("\n" + "=" * 60)
print("SETUP COMPLETE — Copy these values to src/app/app.yaml:")
print("=" * 60)
for k, v in output.items():
    print(f"  {k}: {v}")
print("=" * 60)

dbutils.notebook.exit(json.dumps(output))
