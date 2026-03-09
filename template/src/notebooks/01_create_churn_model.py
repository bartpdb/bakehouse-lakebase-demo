# Databricks notebook source
# MAGIC %md
# MAGIC # Bakehouse Cookie Co. — Churn Predictor Model
# MAGIC
# MAGIC Registers a churn prediction model in Unity Catalog and creates a serving endpoint.
# MAGIC The SDP pipeline's gold layer uses this via `AI_QUERY('cookies-churn-predictor', ...)`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "lakebase_demo")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

if not CATALOG:
    from databricks.sdk import WorkspaceClient as _WC
    _cats = [c for c in _WC().catalogs.list() if c.catalog_type and "MANAGED" in str(c.catalog_type)]
    CATALOG = _cats[0].name if _cats else ""
print(f"Using catalog={CATALOG}, schema={SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register the Churn Model

# COMMAND ----------

import mlflow
import mlflow.pyfunc
import pandas as pd
import random


class ChurnPredictor(mlflow.pyfunc.PythonModel):
    """
    Predicts churn probability based on customer transaction features.
    Customers with fewer transactions have higher churn risk.
    """
    def predict(self, context, model_input: pd.DataFrame):
        def score_row(row):
            tx = row.get("total_transactions", 0)
            spent = row.get("total_spent", 0)
            if tx <= 3:
                return round(0.6 + random.random() * 0.35, 2)
            elif tx <= 6:
                return round(0.35 + random.random() * 0.3, 2)
            elif tx <= 10:
                return round(0.15 + random.random() * 0.25, 2)
            else:
                return round(0.05 + random.random() * 0.2, 2)

        return model_input.apply(score_row, axis=1)


registered_model_name = f"{CATALOG}.{SCHEMA}.churn_predictor"

example_input = pd.DataFrame({
    "total_transactions": [5, 20],
    "total_spent": [100, 102],
    "avg_order_value": [30.5, 45.2],
    "unique_franchises_visited": [2, 5],
    "total_items_purchased": [10, 40],
})

mlflow.set_registry_uri("databricks-uc")

with mlflow.start_run():
    from mlflow.models.signature import infer_signature
    example_output = ChurnPredictor().predict(None, example_input)
    signature = infer_signature(example_input, example_output)

    mlflow.pyfunc.log_model(
        artifact_path="model",
        python_model=ChurnPredictor(),
        registered_model_name=registered_model_name,
        signature=signature,
    )

print(f"Model registered: {registered_model_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Serving Endpoint

# COMMAND ----------

import requests
import json
import time
from urllib.parse import quote
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
host = w.config.host
auth = w.config._header_factory()
headers = {**auth, "Content-Type": "application/json"}

endpoint_name = "cookies-churn-predictor"
model_name = f"{CATALOG}.{SCHEMA}.churn_predictor"

versions_resp = requests.get(
    f"{host}/api/2.1/unity-catalog/models/{quote(model_name, safe='')}/versions",
    headers=auth
).json()

latest_version = max(versions_resp["model_versions"], key=lambda v: int(v["version"]))["version"]
print(f"Latest model version: {latest_version}")

endpoint_config = {
    "name": endpoint_name,
    "config": {
        "served_entities": [{
            "entity_name": model_name,
            "entity_version": latest_version,
            "workload_size": "Small",
            "scale_to_zero_enabled": True
        }]
    }
}

check = requests.get(f"{host}/api/2.0/serving-endpoints/{endpoint_name}", headers=auth)
if check.status_code == 200:
    print(f"Endpoint '{endpoint_name}' already exists, updating...")
    resp = requests.put(
        f"{host}/api/2.0/serving-endpoints/{endpoint_name}/config",
        headers=headers,
        json=endpoint_config["config"]
    )
else:
    print(f"Creating endpoint '{endpoint_name}'...")
    resp = requests.post(
        f"{host}/api/2.0/serving-endpoints",
        headers=headers,
        json=endpoint_config
    )

result = resp.json()
print(f"Endpoint status: {result.get('state', result.get('status', 'unknown'))}")
print(f"Endpoint URL: {host}/serving-endpoints/{endpoint_name}")
