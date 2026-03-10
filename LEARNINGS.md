# Learnings & Pain Points — Bakehouse Lakebase Demo

## Lakebase-Specific Issues

### Table Creation Quirks
- `SERIAL` type not supported — use `BIGINT GENERATED ALWAYS AS IDENTITY`
- `INTEGER` not supported for identity columns — must use `BIGINT`
- `DEFAULT` values not supported without enabling a Delta feature flag — just avoid them
- `UNIQUE` constraints and `REFERENCES` foreign keys may not work — Lakebase runs on Delta under the hood
- Column names from synced tables preserve case (e.g., `customerID`), but tables created via SQL lowercase everything unless quoted

### Authentication
- Lakebase has its own Postgres role system, **separate from Unity Catalog** permissions
- OAuth roles must be explicitly created with `databricks_create_role()` before a service principal can connect
- The `databricks_auth` extension must be enabled first
- Password-based roles can be created via UI; OAuth roles require SQL
- Each new Databricks App gets a **new service principal** — old permissions don't carry over

### Connection Details
- `PGHOST` and `ENDPOINT_NAME` are not obvious to find — they require API calls to `/api/2.0/postgres/projects`, then branches, then endpoints
- The app can auto-discover these at runtime by searching for the project by name

### Synced Tables
- Synced tables create a `lakebase_demo` schema in Lakebase — this schema doesn't exist until the synced table is created
- Permissions on `lakebase_demo` schema can only be granted after the synced table exists
- Synced table creation is **UI-only** — no API available yet
- A synced table in UC shows as `FOREIGN` type (PostgreSQL format), while the source materialized view stays as `MATERIALIZED_VIEW`
- You cannot write to a synced table from the Lakehouse side — it's read-only pointing at Lakebase

## Databricks Apps Issues

### Deployment
- App deletion takes **up to 20 minutes** to propagate — deploying a new app with the same name fails during this window
- Each failed `databricks bundle deploy` can create **ghost resources** (apps, pipelines) that block subsequent deploys
- `databricks bundle destroy` does not reliably delete apps
- Apps need an explicit `databricks apps deploy` with source code path after `bundle deploy` creates them — the bundle creates the app resource but doesn't push source code
- Apps need to be explicitly started; they don't auto-start after creation

### Service Principal Timing
- The app's service principal isn't available immediately after `bundle deploy`
- The setup job runs before the app is fully started, so it can't find the SP
- Solution: split into `setup` (runs first, skips SP if not found) and `finalize` (runs after, handles SP)

## DAB / Bundle Issues

### Pipeline Name Conflicts
- Pipeline names must be unique across the workspace — `databricks bundle deploy` fails if another pipeline with the same name exists (even from a previous failed deploy)
- Ghost pipelines from failed deploys block subsequent deploys
- `allow_duplicate_names: true` in the pipeline config should help but didn't always work in our testing

### Workspace State
- Bundle state lives both locally (`.bundle/`) and in the workspace (`/Workspace/Users/.../.bundle/...`)
- Both must be cleared for a truly fresh start
- `databricks bundle destroy` + `rm -rf .bundle .databricks` + `databricks workspace delete --recursive` on the bundle path

### App Config
- `config` section in `databricks.yml` for apps generates a warning — use `app.yaml` in the source directory instead
- Environment variables in `app.yaml` are static — for dynamic values, the app should auto-discover at runtime

## SDP Pipeline Issues

### Table Names
- `samples.bakehouse.sales_customer` is actually `sales_customers` (with an 's') — no error at pipeline creation, only at runtime

### Churn Model Endpoint
- The serving endpoint takes ~5 minutes to warm up after creation
- The pipeline's `AI_QUERY` will fail if the endpoint isn't ready
- Solution: make `run_pipeline` depend on `create_churn_model` task, and accept it might need a re-run

### Task Ordering
- Genie space creation needs the gold tables to exist (references them)
- Gold tables only exist after the pipeline runs
- Pipeline needs the churn model endpoint
- Correct order: `setup_lakebase` → `create_churn_model` → `run_pipeline` → `create_genie_space`

## Genie Space Issues

### API Quirks
- Tables in `serialized_space` must be **sorted alphabetically** by identifier
- Example SQL IDs must be **32-character lowercase hex** without hyphens, also sorted
- Permission level is `CAN_RUN` (not `CAN_VIEW`)
- The object type for permissions API is `genie` (not `genieSpace` or `genie-space`)
- Re-running the setup creates **duplicate Genie spaces** — must check for existing ones first

### Permissions
- The app's SP needs both `CAN_RUN` on the Genie space AND `SELECT` on all UC tables the space references
- Warehouse `CAN_USE` permission is also required

## Architecture Decisions

### Hybrid gold_live Table
- The gold synced table only updates when the Lakehouse pipeline runs (batch)
- For the demo, we need instant feedback after placing an order
- Solution: `public.gold_live` table merges synced gold data + new transactions from Lakebase
- "Refresh Analytics" does an instant SQL merge, not a pipeline trigger
- Freshness badge shows "Live · updated Xs ago" vs "Synced · timestamp"

### Two-Source Pipeline (Future)
- Currently the SDP pipeline only reads from `samples.bakehouse.*` (Delta Share)
- The plan was to UNION with a foreign table pointing at Lakebase `public.transactions`
- This requires Lakehouse Federation setup which we deferred
- The hybrid `gold_live` approach handles the real-time gap for the demo
