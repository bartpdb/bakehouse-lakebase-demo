# Bakehouse Cookie Co. — Lakebase + Lakehouse Demo

A Customer 360 application showcasing **Lakebase** (OLTP) and the **Databricks Lakehouse** (OLAP) working together. Browse customers, place orders, track churn risk, and ask questions with AI — all powered by a single platform.

## What You Get

- **Customer 360 App** — Browse 300 cookie franchise customers, filter by segment, view analytics
- **Place Orders** — Write transactions directly to Lakebase (real-time OLTP)
- **Churn Alerts** — ML-powered churn predictions with retention offer workflow
- **Instant Analytics Refresh** — Hybrid merge of batch pipeline data + new operational transactions
- **Ask Genie** — Natural language questions about your data (AI/BI)
- **Full Medallion Pipeline** — Bronze → Silver → Gold with data quality checks and ML predictions

## Prerequisites

- Databricks workspace with **Lakebase Autoscaling** enabled
- **Databricks CLI** v0.250+ installed
- `samples.bakehouse` catalog available in the workspace

## Quick Start

```bash
# 1. Authenticate
databricks auth login --host https://your-workspace.cloud.databricks.com

# 2. Find your catalog name
databricks catalogs list
# Look for the MANAGED_CATALOG — copy that name

# 3. Initialize the project
databricks bundle init https://github.com/bartpdb/bakehouse-lakebase-demo
# Enter your catalog name when prompted

# 4. Deploy
cd bakehouse-lakebase-demo
databricks bundle deploy

# 5. Run setup (~10 min)
# Creates: Lakebase project, tables, data, churn model, pipeline, Genie space
databricks bundle run setup

# 6. Create synced table (only manual step)
# In the Databricks UI:
#   Catalog Explorer → your catalog → lakebase_demo → gold_customer_insights
#   Click the table → "Create synced table"
#   Target: bakehouse-lakebase project, databricks_postgres database
#   Name: gold_insights

# 7. Finalize permissions (~1 min)
databricks bundle run finalize

# 8. Deploy the app
databricks apps deploy <app-name> \
  --source-code-path /Workspace/Users/<your-email>/.bundle/bakehouse-lakebase-demo/default/files/src/app

# 9. Open the app
databricks bundle summary
# Click the App URL
```

## Demo Flow

1. Open the app and browse customers
2. Click **High Churn** filter to find at-risk customers
3. Click a customer — see the churn alert banner
4. Click **Place Order** — writes a transaction to Lakebase
5. Click **Refresh Analytics** — churn risk drops instantly, badge shows "Live"
6. Click **Send Retention Offer** — logs the offer in Lakebase
7. Click **Ask Genie** — try "Which customers are most likely to churn?"

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for the full system design and data flow diagrams.

## What the Setup Job Does

| Task | Duration | What it creates |
|------|----------|----------------|
| `setup_lakebase` | ~3 min | SQL Warehouse, Lakebase project, tables, seeds 300 customers + 3333 transactions |
| `create_churn_model` | ~1 min | ML model in UC + serving endpoint |
| `run_pipeline` | ~5 min | Full medallion pipeline (bronze → silver → gold with AI_QUERY churn) |
| `create_genie_space` | ~30s | AI/BI Genie space with curated questions |

After creating the synced table:

| Task | Duration | What it does |
|------|----------|-------------|
| `finalize` | ~1 min | Creates OAuth role, grants all permissions, seeds gold_live |

## File Structure

```
databricks.yml                          # Bundle configuration
databricks_template_schema.json         # Template prompts for bundle init
src/
  app/
    app.py                              # Flask Customer 360 app
    app.yaml                            # App runtime config
    requirements.txt                    # Python dependencies
  pipeline/
    cookies_sdp_pipeline.sql            # SDP medallion pipeline
  notebooks/
    00_setup_lakebase.py                # Lakebase project, tables, data
    01_create_churn_model.py            # ML model + serving endpoint
    02_create_genie_space.py            # Genie AI/BI space
    03_finalize_permissions.py          # Post-sync permissions + gold_live
template/                               # Bundle template for databricks bundle init
```

## Troubleshooting

**Setup job fails at `setup_lakebase`:**
- Check that Lakebase Autoscaling is enabled on your workspace
- Check that `samples.bakehouse` catalog exists

**Pipeline fails at `AI_QUERY`:**
- The churn model serving endpoint takes ~5 minutes to warm up
- Wait and re-run: `databricks bundle run setup`

**App shows "Loading customers..." forever:**
- Run `databricks bundle run finalize` — the OAuth role may not have been created
- Check that the synced table `gold_insights` exists in Lakebase

**Genie returns errors:**
- Ensure the app's service principal has `CAN_RUN` on the Genie space
- Run `databricks bundle run finalize` to fix permissions

**`bundle deploy` fails with "already exists":**
- Ghost resources from failed deploys — delete them in the workspace UI
- For apps: wait 20 minutes after deletion before recreating with the same name
