# Databricks notebook source
# MAGIC %md
# MAGIC # Bakehouse Cookie Co. — Finalize Permissions
# MAGIC
# MAGIC Runs after the SDP pipeline + synced table creation to grant the app service
# MAGIC principal access to the `lakebase_demo` schema in Lakebase (which only exists
# MAGIC after the pipeline has run and the synced table is created).

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
    CATALOG = catalogs[0].name if catalogs else ""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Find App Service Principal

# COMMAND ----------

apps_resp = requests.get(f"{host}/api/2.0/apps", headers=api_headers())
apps = apps_resp.json().get("apps", [])
sp_id = None
for a in apps:
    if "lakebase" in a.get("name", "").lower() or "c360" in a.get("name", "").lower():
        sp_id = a.get("service_principal_client_id")
        print(f"Found app: {a['name']} with SP: {sp_id}")
        break

if not sp_id:
    print("No app found — skipping permission grants")
    dbutils.notebook.exit(json.dumps({"status": "skipped", "reason": "no app found"}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discover Lakebase Connection

# COMMAND ----------

projects_resp = requests.get(f"{host}/api/2.0/postgres/projects", headers=api_headers())
projects = projects_resp.json().get("projects", [])
project = next((p for p in projects if PROJECT_NAME in p.get("name", "")), None)

if not project:
    print("No Lakebase project found — skipping")
    dbutils.notebook.exit(json.dumps({"status": "skipped", "reason": "no project"}))

full_project_name = project["name"] if project["name"].startswith("projects/") else f"projects/{project['name']}"
branches = requests.get(f"{host}/api/2.0/postgres/{full_project_name}/branches", headers=api_headers()).json().get("branches", [])
branch_name = branches[0]["name"]
endpoints = requests.get(f"{host}/api/2.0/postgres/{branch_name}/endpoints", headers=api_headers()).json().get("endpoints", [])
endpoint = endpoints[0]

PGHOST = endpoint["status"]["hosts"]["host"]
ENDPOINT_NAME = endpoint["name"]
print(f"Lakebase: {PGHOST}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant Lakebase Permissions

# COMMAND ----------

cred = w.postgres.generate_database_credential(endpoint=ENDPOINT_NAME)
conn = psycopg.connect(
    host=PGHOST, port=5432, dbname=PGDATABASE,
    user=user, password=cred.token, sslmode="require"
)
cur = conn.cursor()

grants = [
    f'GRANT CONNECT ON DATABASE {PGDATABASE} TO "{sp_id}"',
    f'GRANT USAGE ON SCHEMA public TO "{sp_id}"',
    f'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO "{sp_id}"',
    f'GRANT USAGE ON SCHEMA lakebase_demo TO "{sp_id}"',
    f'GRANT SELECT ON ALL TABLES IN SCHEMA lakebase_demo TO "{sp_id}"',
]

for g in grants:
    try:
        cur.execute(g)
        conn.commit()
        print(f"  OK: {g[:70]}...")
    except Exception as e:
        conn.rollback()
        print(f"  SKIP: {e}")

conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant UC + Warehouse Permissions

# COMMAND ----------

for stmt in [
    f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `{sp_id}`",
    f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{SCHEMA} TO `{sp_id}`",
    f"GRANT SELECT ON SCHEMA {CATALOG}.{SCHEMA} TO `{sp_id}`",
]:
    try:
        warehouses = list(w.warehouses.list())
        wh_id = warehouses[0].id
        w.statement_execution.execute_statement(warehouse_id=wh_id, statement=stmt, wait_timeout="30s")
        print(f"  UC OK: {stmt[:70]}...")
    except Exception as e:
        print(f"  UC SKIP: {e}")

requests.patch(
    f"{host}/api/2.0/permissions/warehouses/{wh_id}",
    headers=api_headers(),
    json={"access_control_list": [{"service_principal_name": sp_id, "permission_level": "CAN_USE"}]}
)

# Grant Genie access
spaces = requests.get(f"{host}/api/2.0/genie/spaces", headers=api_headers()).json()
for s in spaces.get("spaces", []):
    if "Bakehouse" in s.get("title", ""):
        requests.patch(
            f"{host}/api/2.0/permissions/genie/{s['space_id']}",
            headers=api_headers(),
            json={"access_control_list": [{"service_principal_name": sp_id, "permission_level": "CAN_RUN"}]}
        )
        print(f"  Genie OK: {s['space_id']}")
        break

print("\nAll permissions finalized!")
dbutils.notebook.exit(json.dumps({"status": "complete", "service_principal": sp_id}))
