# Architecture — Bakehouse Cookie Co. Customer 360

## Overview

This demo combines **Lakebase** (OLTP) and the **Databricks Lakehouse** (OLAP) into a single Customer 360 application, showing how operational and analytical data work together.

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATABRICKS APPS                             │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │  Flask App — Customer 360                                     │   │
│  │                                                               │   │
│  │  • Browse 300 customers with search & segment filters         │   │
│  │  • View analytics: LTV, churn risk, order history             │   │
│  │  • Place orders (writes to Lakebase)                          │   │
│  │  • Send retention offers (writes to Lakebase)                 │   │
│  │  • Refresh Analytics (instant merge of synced + new data)     │   │
│  │  • Ask Genie (natural language queries via AI/BI)             │   │
│  └──────────┬──────────────────────────┬─────────────────────────┘   │
│             │                          │                             │
└─────────────┼──────────────────────────┼─────────────────────────────┘
              │ reads KPIs               │ writes orders
              ▼                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                          LAKEBASE (OLTP)                            │
│                                                                     │
│  public schema (operational):          lakebase_demo schema:        │
│  ┌─────────────┐ ┌──────────────┐     ┌──────────────────┐         │
│  │ customers   │ │ transactions │     │ gold_insights    │◄─sync─┐ │
│  │ (300 rows)  │ │ (3333+ rows) │     │ (synced table)   │       │ │
│  └─────────────┘ └──────────────┘     └──────────────────┘       │ │
│  ┌─────────────┐ ┌──────────────┐                                │ │
│  │ franchises  │ │ supplies     │     ┌──────────────────┐       │ │
│  │ (48 rows)   │ │ (27 rows)    │     │ gold_live        │       │ │
│  └─────────────┘ └──────────────┘     │ (hybrid: synced  │       │ │
│  ┌──────────────────┐                 │  + new tx merge) │       │ │
│  │ retention_offers  │                 └──────────────────┘       │ │
│  └──────────────────┘                                            │ │
└──────────────────────────────────────────────────────────────────┼──┘
                                                                   │
              ┌────────────────────────────────────────────────────┘
              │ synced table
              │
┌─────────────┼───────────────────────────────────────────────────────┐
│             │        LAKEHOUSE (OLAP)                                │
│             │                                                       │
│  ┌──────────┴────────────────────────────────────────────────────┐  │
│  │  SDP Pipeline (Medallion Architecture)                         │  │
│  │                                                                │  │
│  │  Bronze (temp views)          Silver (MVs)      Gold (MVs)     │  │
│  │  ┌──────────────────┐   ┌─────────────┐   ┌────────────────┐  │  │
│  │  │ samples.bakehouse│──▶│ silver_     │──▶│ gold_customer  │  │  │
│  │  │ .sales_*         │   │ customers   │   │ _insights      │──┼──┘
│  │  │ (Delta Share)    │   │ transactions│   │ (+ AI_QUERY    │  │
│  │  └──────────────────┘   │ franchises  │   │  churn model)  │  │
│  │                         │ supplies    │   │                │  │
│  │                         └─────────────┘   │ gold_franchise │  │
│  │                                           │ _performance   │  │
│  │                                           │                │  │
│  │                                           │ gold_product   │  │
│  │                                           │ _analytics     │  │
│  │                                           └────────────────┘  │
│  └───────────────────────────────────────────────────────────────┘  │
│                                                                     │
│  ┌───────────────────┐  ┌──────────────────────────────────────┐   │
│  │ Churn Predictor   │  │ Genie Space (AI/BI)                  │   │
│  │ (ML Serving       │  │ Natural language queries over         │   │
│  │  Endpoint)        │  │ gold + silver tables                  │   │
│  └───────────────────┘  └──────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

## Data Flow

### Write Path (operational)
1. User places order in the app
2. App writes to `public.transactions` in Lakebase via OAuth + psycopg
3. Transaction is immediately visible in the customer's order history

### Read Path (analytical)
1. App reads customer KPIs from `public.gold_live` in Lakebase
2. `gold_live` contains a merge of:
   - **Synced data**: from `lakebase_demo.gold_insights` (Lakehouse pipeline output)
   - **New transactions**: any orders placed after the last pipeline run
3. "Refresh Analytics" recomputes this merge instantly

### Batch Path (pipeline)
1. SDP pipeline reads from `samples.bakehouse.*` (Delta Share)
2. Bronze → Silver (cleaned + quality checks) → Gold (aggregated + ML churn)
3. `gold_customer_insights` materialized view includes `AI_QUERY` churn predictions
4. Synced table pushes gold data to Lakebase `lakebase_demo.gold_insights`

### AI/BI Path (Genie)
1. User asks natural language question in the app
2. App calls Genie Conversation API with the question
3. Genie generates SQL, runs it on the SQL Warehouse
4. Results displayed in the chat panel

## Component Map

| Component | Technology | Purpose |
|-----------|-----------|---------|
| App | Flask + Databricks Apps | Customer 360 UI |
| Lakebase | PostgreSQL (Databricks managed) | Operational data store |
| Pipeline | Lakeflow SDP (SQL) | Medallion architecture ETL |
| Churn Model | MLflow + Model Serving | ML predictions via AI_QUERY |
| Genie | AI/BI Genie Spaces | Natural language analytics |
| Auth | OAuth + databricks_auth | Automatic credential rotation |

## DAB Resources

| Resource | Type | Purpose |
|----------|------|---------|
| `cookies_analytics` | Pipeline | SDP medallion pipeline |
| `setup` | Job (4 tasks) | One-time setup: Lakebase, model, pipeline, Genie |
| `finalize` | Job (1 task) | Post-sync permissions + gold_live seeding |
| `customer360` | App | The Flask application |

## Setup Task Ordering

```
setup_lakebase ──► create_churn_model ──► run_pipeline ──► create_genie_space
     │                    │                     │                   │
     │                    │                     │                   │
  Creates:             Creates:              Runs:              Creates:
  • Lakebase project   • ML model in UC      • Bronze→Silver    • Genie space
  • Operational tables  • Serving endpoint     →Gold MVs         • SP permissions
  • Seeds data                                • AI_QUERY churn
  • Warehouse (if needed)
```

Then manually: **Create synced table** (`gold_customer_insights` → `gold_insights`)

Then: `databricks bundle run finalize`
- Creates OAuth role for app SP
- Grants Lakebase + UC + Genie permissions
- Seeds `gold_live` from synced table
