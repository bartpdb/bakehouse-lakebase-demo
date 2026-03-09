# Databricks notebook source
# MAGIC %md
# MAGIC # Bakehouse Cookie Co. — Genie Space Setup
# MAGIC
# MAGIC Creates an AI/BI Genie space with curated instructions and sample questions,
# MAGIC then grants the app's service principal access.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "lakebase_demo")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Genie Space

# COMMAND ----------

import json
import requests
import uuid
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
host = w.config.host
auth = w.config._header_factory()

if not CATALOG:
    catalogs = [c for c in w.catalogs.list() if c.catalog_type and "MANAGED" in str(c.catalog_type)]
    CATALOG = catalogs[0].name if catalogs else ""

warehouses = list(w.warehouses.list())
WAREHOUSE_ID = warehouses[0].id if warehouses else ""
if not WAREHOUSE_ID:
    raise RuntimeError("No SQL warehouse found")
print(f"Using catalog={CATALOG}, warehouse={WAREHOUSE_ID}")

tables = sorted([
    f"{CATALOG}.{SCHEMA}.gold_customer_insights",
    f"{CATALOG}.{SCHEMA}.gold_franchise_performance",
    f"{CATALOG}.{SCHEMA}.gold_product_analytics",
    f"{CATALOG}.{SCHEMA}.silver_customers",
    f"{CATALOG}.{SCHEMA}.silver_franchises",
    f"{CATALOG}.{SCHEMA}.silver_transactions",
])

serialized_space = json.dumps({
    "version": 1,
    "data_sources": {
        "tables": [{"identifier": t} for t in tables]
    },
    "instructions": {
        "text_instructions": [{
            "id": "a0000000000000000000000000000000",
            "content": [
                "This Genie space is for Bakehouse Cookie Co., a cookie franchise business.\n",
                "\n",
                "Data model:\n",
                f"- {CATALOG}.{SCHEMA}.gold_customer_insights: customer-level analytics including lifetime value (total_spent), churn_probability, customer_value_segment (High/Medium/Low Value), customer_connection_segment, total_transactions, avg_order_value, days_since_last_purchase, unique_franchises_visited\n",
                f"- {CATALOG}.{SCHEMA}.gold_franchise_performance: franchise-level KPIs including total_revenue, unique_customers, total_transactions, avg_transaction_value, plus last 30-day metrics\n",
                f"- {CATALOG}.{SCHEMA}.gold_product_analytics: product-level metrics including total_revenue, total_quantity_sold, unique_customers, avg_unit_price, plus last 30-day metrics\n",
                f"- {CATALOG}.{SCHEMA}.silver_transactions: individual transaction records with customerID, franchiseID, product, quantity, unitPrice, totalPrice, payment_method, dateTime\n",
                f"- {CATALOG}.{SCHEMA}.silver_customers: customer master data with name, email, phone, address, city, country, gender\n",
                f"- {CATALOG}.{SCHEMA}.silver_franchises: franchise locations with franchise_name, city, district, country, size, lat/long\n",
                "\n",
                "Business context:\n",
                "- Products: Austin Almond Biscotti, Golden Gate Ginger, Orchard Oasis, Outback Oatmeal, Pearly Pies, Tokyo Tidbits\n",
                "- Customer segments are based on percentile ranking of total_spent (High/Medium/Low Value) and unique_franchises_visited (High/Medium/Low Connection)\n",
                "- Churn probability is predicted by a ML model (cookies-churn-predictor) based on transaction features\n",
                "- Currency is EUR\n",
                "- 300 customers across multiple countries, 48 franchise locations, ~3300+ transactions\n",
                "\n",
                "Tips:\n",
                "- Use gold tables for aggregated insights and KPIs\n",
                "- Use silver tables for drill-downs into individual transactions or customer details\n",
                "- Franchise performance can be compared by city, country, or size\n",
                "- For revenue trends, group silver_transactions by transaction_year and transaction_month\n",
            ]
        }],
        "example_question_sqls": [
            {"id": "a0000000000000000000000000000001", "question": ["Which customers are most likely to churn?"], "sql": [
                f"SELECT full_name, city, total_spent, churn_probability, customer_value_segment FROM {CATALOG}.{SCHEMA}.gold_customer_insights WHERE churn_probability > 0.5 ORDER BY churn_probability DESC LIMIT 20;"
            ]},
            {"id": "a0000000000000000000000000000002", "question": ["Top franchises by revenue?"], "sql": [
                f"SELECT franchise_name, city, total_revenue, unique_customers FROM {CATALOG}.{SCHEMA}.gold_franchise_performance ORDER BY total_revenue DESC LIMIT 10;"
            ]},
            {"id": "a0000000000000000000000000000003", "question": ["Which products generate the most revenue?"], "sql": [
                f"SELECT product, total_revenue, total_quantity_sold, avg_unit_price FROM {CATALOG}.{SCHEMA}.gold_product_analytics ORDER BY total_revenue DESC;"
            ]},
            {"id": "a0000000000000000000000000000004", "question": ["Revenue trend by month"], "sql": [
                f"SELECT transaction_year, transaction_month, SUM(totalPrice) as monthly_revenue, COUNT(*) as num_transactions FROM {CATALOG}.{SCHEMA}.silver_transactions GROUP BY transaction_year, transaction_month ORDER BY 1, 2;"
            ]},
            {"id": "a0000000000000000000000000000005", "question": ["Customer segment distribution"], "sql": [
                f"SELECT customer_value_segment, COUNT(*) as num_customers, ROUND(AVG(total_spent), 2) as avg_ltv, ROUND(AVG(churn_probability), 3) as avg_churn FROM {CATALOG}.{SCHEMA}.gold_customer_insights GROUP BY customer_value_segment ORDER BY avg_ltv DESC;"
            ]},
        ]
    }
})

resp = requests.post(
    f"{host}/api/2.0/genie/spaces",
    headers={**auth, "Content-Type": "application/json"},
    json={
        "title": "Bakehouse Cookie Co. Analytics",
        "description": "Ask questions about customers, transactions, franchises and product performance",
        "warehouse_id": WAREHOUSE_ID,
        "serialized_space": serialized_space
    }
)

result = resp.json()
if "space_id" in result:
    space_id = result["space_id"]
    print(f"Genie Space created: {space_id}")
    print(f"URL: {host}/genie/rooms/{space_id}")
else:
    print(f"Error: {json.dumps(result, indent=2)[:500]}")
    dbutils.notebook.exit(json.dumps({"error": "Failed to create Genie space"}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant Permissions

# COMMAND ----------

# Find app service principal
apps_resp = requests.get(f"{host}/api/2.0/apps", headers=auth)
apps = apps_resp.json().get("apps", [])
sp_id = None
for a in apps:
    if "lakebase" in a.get("name", "").lower() or "c360" in a.get("name", "").lower():
        sp_id = a.get("service_principal_client_id")
        break

if sp_id:
    resp = requests.patch(
        f"{host}/api/2.0/permissions/genie/{space_id}",
        headers={**auth, "Content-Type": "application/json"},
        json={"access_control_list": [
            {"service_principal_name": sp_id, "permission_level": "CAN_RUN"}
        ]}
    )
    print(f"Granted CAN_RUN to {sp_id}: {resp.status_code}")
else:
    print("WARNING: App service principal not found. Grant Genie permissions manually.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Output Space ID

# COMMAND ----------

dbutils.notebook.exit(json.dumps({"genie_space_id": space_id}))
