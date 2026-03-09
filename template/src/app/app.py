import os
import time
import datetime
import requests as http_requests
from databricks.sdk import WorkspaceClient
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from flask import Flask, jsonify, request

app = Flask(__name__)

w = WorkspaceClient()


class OAuthConnection(psycopg.Connection):
    @classmethod
    def connect(cls, conninfo='', **kwargs):
        endpoint_name = os.environ["ENDPOINT_NAME"]
        credential = w.postgres.generate_database_credential(endpoint=endpoint_name)
        kwargs['password'] = credential.token
        return super().connect(conninfo, **kwargs)


username = os.environ.get("PGUSER") or os.environ.get("DATABRICKS_CLIENT_ID") or w.current_user.me().user_name
host = os.environ["PGHOST"]
port = os.environ.get("PGPORT", "5432")
database = os.environ["PGDATABASE"]
sslmode = os.environ.get("PGSSLMODE", "require")

pool = ConnectionPool(
    conninfo=f"dbname={database} user={username} host={host} port={port} sslmode={sslmode}",
    connection_class=OAuthConnection,
    min_size=1,
    max_size=10,
    kwargs={"row_factory": dict_row},
    open=True
)


GOLD_SYNCED = "lakebase_demo.gold_insights"
GOLD_LIVE = "public.gold_live"


# ---------------------------------------------------------------------------
# API: Customers
# ---------------------------------------------------------------------------

@app.route("/api/customers")
def api_customers():
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT "customerID" as id, first_name, last_name, full_name,
                       email_address, city, country,
                       customer_value_segment, customer_connection_segment,
                       churn_probability, total_spent as lifetime_value,
                       total_transactions, updated_at, source
                FROM {GOLD_LIVE}
                ORDER BY last_name, first_name
            """)
            customers = cur.fetchall()
    return jsonify(customers)


@app.route("/api/customers/<int:customer_id>")
def api_customer_detail(customer_id):
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT "customerID" as id, first_name, last_name, full_name,
                       email_address, phone_number, city, state, country, continent, gender,
                       total_transactions, total_spent as lifetime_value,
                       avg_order_value, days_since_last_purchase,
                       unique_franchises_visited, total_items_purchased,
                       customer_value_segment, customer_connection_segment,
                       first_purchase_date, last_purchase_date,
                       churn_probability, updated_at, source
                FROM {GOLD_LIVE}
                WHERE "customerID" = %s
            """, (customer_id,))

            customer = cur.fetchone()

            cur.execute("""
                SELECT t.transactionID, t.dateTime as order_date,
                       t.product, t.quantity, t.unitPrice, t.totalPrice,
                       t.payment_method,
                       f.franchise_name, f.city as franchise_city
                FROM transactions t
                LEFT JOIN franchises f ON f.franchiseID = t.franchiseID
                WHERE t.customerID = %s
                ORDER BY t.dateTime DESC
                LIMIT 20
            """, (customer_id,))
            transactions = cur.fetchall()

            cur.execute("""
                SELECT id, offer_type, offer_detail, status, created_at
                FROM retention_offers
                WHERE customerID = %s
                ORDER BY created_at DESC
                LIMIT 5
            """, (customer_id,))
            offers = cur.fetchall()

    if not customer:
        return jsonify({"error": "Customer not found"}), 404

    customer["transactions"] = transactions
    customer["retention_offers"] = offers
    return jsonify(customer)


# ---------------------------------------------------------------------------
# API: Place Order (OLTP write to Lakebase)
# ---------------------------------------------------------------------------

@app.route("/api/orders", methods=["POST"])
def api_place_order():
    data = request.json
    customer_id = data.get("customer_id")
    franchise_id = data.get("franchise_id")
    product = data.get("product")
    quantity = data.get("quantity", 1)
    unit_price = data.get("unit_price", 5)

    if not all([customer_id, franchise_id, product]):
        return jsonify({"error": "Missing required fields"}), 400

    total_price = quantity * unit_price
    now = datetime.datetime.now()

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(MAX(transactionID), 0) + 1 AS next_id FROM transactions")
            next_id = cur.fetchone()["next_id"]
            cur.execute("""
                INSERT INTO transactions
                    (transactionID, customerID, franchiseID, dateTime, transaction_date, product,
                     quantity, unitPrice, totalPrice, payment_method,
                     transaction_year, transaction_month, day_of_week, hour_of_day)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                next_id, customer_id, franchise_id, now, now.date(), product,
                quantity, unit_price, total_price, data.get("payment_method", "Card"),
                now.year, now.month, now.weekday(), now.hour
            ))
        conn.commit()

    return jsonify({"success": True, "message": f"Order placed: {quantity}x {product} = €{total_price}"})


# ---------------------------------------------------------------------------
# API: Send Retention Offer (OLTP write)
# ---------------------------------------------------------------------------

@app.route("/api/retention-offers", methods=["POST"])
def api_send_retention_offer():
    data = request.json
    customer_id = data.get("customer_id")
    offer_type = data.get("offer_type", "Discount")
    offer_detail = data.get("offer_detail", "20% off your next order")

    if not customer_id:
        return jsonify({"error": "Missing customer_id"}), 400

    now = datetime.datetime.now()

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO retention_offers (customerID, offer_type, offer_detail, status, created_at)
                VALUES (%s, %s, %s, %s, %s)
            """, (customer_id, offer_type, offer_detail, "sent", now))
        conn.commit()

    return jsonify({"success": True, "message": f"Retention offer sent: {offer_type}"})


# ---------------------------------------------------------------------------
# API: Refresh Analytics (trigger Databricks pipeline)
# ---------------------------------------------------------------------------

REFRESH_JOB_ID = os.environ.get("REFRESH_JOB_ID", "")


@app.route("/api/refresh-analytics", methods=["POST"])
def api_refresh_analytics():
    """Instant merge of synced gold + new transactions, then trigger pipeline in background."""
    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    INSERT INTO {GOLD_LIVE} (
                        "customerID", full_name, first_name, last_name,
                        email_address, phone_number, city, state, country, continent, gender,
                        total_transactions, total_spent, avg_order_value,
                        days_since_last_purchase, unique_franchises_visited, total_items_purchased,
                        customer_value_segment, customer_connection_segment,
                        first_purchase_date, last_purchase_date, churn_probability,
                        updated_at, source
                    )
                    SELECT
                        g."customerID", g.full_name, g.first_name, g.last_name,
                        g.email_address, g.phone_number, g.city, g.state,
                        g.country, g.continent, g.gender,
                        COALESCE(g.total_transactions, 0) + COALESCE(t.new_tx, 0),
                        COALESCE(g.total_spent, 0) + COALESCE(t.new_spent, 0),
                        CASE WHEN COALESCE(g.total_transactions, 0) + COALESCE(t.new_tx, 0) > 0
                             THEN ROUND((COALESCE(g.total_spent, 0) + COALESCE(t.new_spent, 0))::numeric
                                  / (COALESCE(g.total_transactions, 0) + COALESCE(t.new_tx, 0)), 2)
                             ELSE 0 END,
                        CASE WHEN t.last_new_tx IS NOT NULL
                             THEN EXTRACT(DAY FROM NOW() - t.last_new_tx)::int
                             ELSE g.days_since_last_purchase END,
                        GREATEST(COALESCE(g.unique_franchises_visited, 0), COALESCE(t.new_franchises, 0)),
                        COALESCE(g.total_items_purchased, 0) + COALESCE(t.new_items, 0),
                        CASE WHEN COALESCE(g.total_spent, 0) + COALESCE(t.new_spent, 0) >= 100 THEN 'High Value'
                             WHEN COALESCE(g.total_spent, 0) + COALESCE(t.new_spent, 0) >= 40 THEN 'Medium Value'
                             ELSE 'Low Value' END,
                        g.customer_connection_segment,
                        COALESCE(g.first_purchase_date, t.first_new_tx),
                        COALESCE(t.last_new_tx, g.last_purchase_date),
                        CASE
                            WHEN t.last_new_tx IS NOT NULL AND EXTRACT(DAY FROM NOW() - t.last_new_tx) <= 7 THEN 0.05
                            WHEN t.last_new_tx IS NOT NULL AND EXTRACT(DAY FROM NOW() - t.last_new_tx) <= 30 THEN 0.15
                            WHEN COALESCE(g.days_since_last_purchase, 999) <= 7 THEN 0.05
                            WHEN COALESCE(g.days_since_last_purchase, 999) <= 30 THEN 0.15
                            WHEN COALESCE(g.days_since_last_purchase, 999) <= 60 THEN 0.35
                            WHEN COALESCE(g.days_since_last_purchase, 999) <= 90 THEN 0.55
                            ELSE 0.80
                        END,
                        NOW(),
                        'live'
                    FROM {GOLD_SYNCED} g
                    LEFT JOIN (
                        SELECT customerid,
                               COUNT(*) as new_tx,
                               SUM(totalprice) as new_spent,
                               SUM(quantity) as new_items,
                               COUNT(DISTINCT franchiseid) as new_franchises,
                               MIN(datetime) as first_new_tx,
                               MAX(datetime) as last_new_tx
                        FROM transactions
                        WHERE datetime > (SELECT COALESCE(MAX(last_purchase_date), '1970-01-01') FROM {GOLD_SYNCED})
                        GROUP BY customerid
                    ) t ON g."customerID" = t.customerid
                    ON CONFLICT ("customerID") DO UPDATE SET
                        total_transactions = EXCLUDED.total_transactions,
                        total_spent = EXCLUDED.total_spent,
                        avg_order_value = EXCLUDED.avg_order_value,
                        days_since_last_purchase = EXCLUDED.days_since_last_purchase,
                        unique_franchises_visited = EXCLUDED.unique_franchises_visited,
                        total_items_purchased = EXCLUDED.total_items_purchased,
                        customer_value_segment = EXCLUDED.customer_value_segment,
                        first_purchase_date = EXCLUDED.first_purchase_date,
                        last_purchase_date = EXCLUDED.last_purchase_date,
                        churn_probability = EXCLUDED.churn_probability,
                        updated_at = EXCLUDED.updated_at,
                        source = EXCLUDED.source
                """)
            conn.commit()

        return jsonify({"success": True, "message": "Analytics updated with latest transactions."})
    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {str(e)}"})


# ---------------------------------------------------------------------------
# API: Reference data
# ---------------------------------------------------------------------------

@app.route("/api/franchises")
def api_franchises():
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT franchiseID as id, franchise_name, city FROM franchises ORDER BY franchise_name")
            return jsonify(cur.fetchall())


@app.route("/api/products")
def api_products():
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT product FROM transactions ORDER BY product")
            return jsonify([r["product"] for r in cur.fetchall()])


# ---------------------------------------------------------------------------
# API: Genie (AI/BI natural language queries)
# ---------------------------------------------------------------------------

GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID", "")


def genie_headers():
    auth = w.config._header_factory()
    return {**auth, "Content-Type": "application/json"}


@app.route("/api/genie/ask", methods=["POST"])
def api_genie_ask():
    data = request.json
    question = data.get("question", "")
    if not question:
        return jsonify({"error": "No question provided"}), 400

    host = w.config.host
    hdrs = genie_headers()

    resp = http_requests.post(
        f"{host}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/start-conversation",
        headers=hdrs,
        json={"content": question}
    )
    if resp.status_code != 200:
        return jsonify({"error": f"Genie API error: {resp.text}"}), 500

    conv = resp.json()
    conversation_id = conv.get("conversation_id", "")
    message_id = conv.get("message_id", "")

    if not conversation_id or not message_id:
        return jsonify({"error": "Failed to start conversation", "raw": conv}), 500

    result = poll_genie_message(host, hdrs, conversation_id, message_id)
    return jsonify(result)


@app.route("/api/genie/followup", methods=["POST"])
def api_genie_followup():
    data = request.json
    conversation_id = data.get("conversation_id", "")
    question = data.get("question", "")

    host = w.config.host
    hdrs = genie_headers()

    resp = http_requests.post(
        f"{host}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations/{conversation_id}/messages",
        headers=hdrs,
        json={"content": question}
    )
    if resp.status_code != 200:
        return jsonify({"error": f"Genie API error: {resp.text}"}), 500

    msg = resp.json()
    message_id = msg.get("message_id", "")
    result = poll_genie_message(host, hdrs, conversation_id, message_id)
    return jsonify(result)


def poll_genie_message(host, hdrs, conversation_id, message_id):
    for _ in range(30):
        resp = http_requests.get(
            f"{host}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations/{conversation_id}/messages/{message_id}",
            headers=hdrs
        )
        if resp.status_code != 200:
            return {"error": f"Poll error: {resp.text}", "conversation_id": conversation_id}

        msg = resp.json()
        status = msg.get("status", "")

        if status in ("COMPLETED", "COMPLETE"):
            attachments = msg.get("attachments", [])
            text_content = ""
            query_result = None
            sql_query = None

            for att in attachments:
                if att.get("text"):
                    text_content += att["text"].get("content", "")
                if att.get("query"):
                    sql_query = att["query"].get("query", "")
                    att_id = att.get("id", "")
                    if att_id:
                        qr_url = f"{host}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations/{conversation_id}/messages/{message_id}/query-result/{att_id}"
                        qr = http_requests.get(qr_url, headers=hdrs)
                        if qr.status_code == 200:
                            query_result = qr.json()

            if not text_content and not sql_query and not query_result:
                text_content = msg.get("content", "No response generated")

            return {
                "conversation_id": conversation_id,
                "message_id": message_id,
                "text": text_content,
                "sql": sql_query,
                "query_result": query_result,
                "status": "completed"
            }

        if status in ("FAILED", "CANCELLED"):
            return {"error": msg.get("error", "Query failed"), "status": status, "conversation_id": conversation_id}

        time.sleep(2)

    return {"error": "Timeout waiting for Genie response", "conversation_id": conversation_id}


# ---------------------------------------------------------------------------
# Frontend
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return INDEX_HTML


INDEX_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Bakehouse Cookie Co. — Customer 360</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f5f3f0; color: #2d2926; height: 100vh; display: flex; flex-direction: column; }

  header { background: #2d2926; color: #f5f3f0; padding: 14px 24px; display: flex; align-items: center; gap: 12px; }
  header h1 { font-size: 20px; font-weight: 600; }
  header .subtitle { font-size: 13px; color: #a89e97; margin-left: 4px; }
  header .spacer { flex: 1; }
  header .pill { font-size: 11px; background: rgba(196,131,74,0.2); color: #c4834a; padding: 4px 10px; border-radius: 10px; }
  .refresh-btn { background: #c4834a; color: #fff; border: none; padding: 7px 14px; border-radius: 6px; font-size: 12px; font-weight: 500; cursor: pointer; display: flex; align-items: center; gap: 6px; transition: all 0.2s; }
  .refresh-btn:hover { background: #b3743f; }
  .refresh-btn:disabled { opacity: 0.6; cursor: not-allowed; }
  .refresh-btn svg { width: 14px; height: 14px; }
  .refresh-btn.spinning svg { animation: spin 1s linear infinite; }
  @keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }

  .container { display: flex; flex: 1; overflow: hidden; }

  .sidebar { width: 380px; min-width: 380px; background: #fff; border-right: 1px solid #e5e0db; display: flex; flex-direction: column; }
  .sidebar-controls { padding: 12px 16px; border-bottom: 1px solid #e5e0db; display: flex; flex-direction: column; gap: 8px; }
  .sidebar-controls input { width: 100%; padding: 8px 12px; border: 1px solid #d4cfc9; border-radius: 6px; font-size: 14px; outline: none; }
  .sidebar-controls input:focus { border-color: #c4834a; box-shadow: 0 0 0 2px rgba(196,131,74,0.15); }
  .filter-row { display: flex; gap: 6px; flex-wrap: wrap; }
  .filter-chip { padding: 4px 10px; border-radius: 12px; font-size: 11px; font-weight: 500; border: 1px solid #d4cfc9; background: #fff; cursor: pointer; transition: all 0.15s; }
  .filter-chip:hover { border-color: #c4834a; }
  .filter-chip.active { background: #2d2926; color: #fff; border-color: #2d2926; }
  .filter-chip.churn { border-color: #fca5a5; }
  .filter-chip.churn.active { background: #dc2626; border-color: #dc2626; }
  .customer-list { flex: 1; overflow-y: auto; }
  .customer-item { padding: 12px 16px; border-bottom: 1px solid #f0ece8; cursor: pointer; transition: background 0.15s; display: flex; align-items: center; gap: 12px; }
  .customer-item:hover { background: #faf8f6; }
  .customer-item.active { background: #fdf6ef; border-left: 3px solid #c4834a; }
  .avatar { width: 36px; height: 36px; border-radius: 50%; background: #c4834a; color: #fff; display: flex; align-items: center; justify-content: center; font-weight: 600; font-size: 14px; flex-shrink: 0; }
  .avatar.lg { width: 56px; height: 56px; font-size: 22px; font-weight: 700; }
  .customer-info h3 { font-size: 14px; font-weight: 600; }
  .customer-info p { font-size: 12px; color: #8a8178; margin-top: 2px; }
  .customer-meta { margin-left: auto; text-align: right; flex-shrink: 0; }
  .customer-meta .ltv { font-size: 13px; font-weight: 600; color: #2d2926; }
  .badge { font-size: 11px; padding: 2px 8px; border-radius: 10px; font-weight: 500; display: inline-block; margin-top: 2px; }
  .badge-premium { background: #fef3c7; color: #92400e; }
  .badge-high { background: #d1fae5; color: #065f46; }
  .badge-medium { background: #dbeafe; color: #1e40af; }
  .badge-low { background: #e0e7ff; color: #3730a3; }
  .badge-churning { background: #fee2e2; color: #991b1b; }
  .freshness { font-size: 11px; color: #8a8178; margin-left: 12px; display: inline-flex; align-items: center; gap: 4px; }
  .freshness .dot { width: 6px; height: 6px; border-radius: 50%; display: inline-block; }
  .freshness .dot.live { background: #10b981; }
  .freshness .dot.synced { background: #6366f1; }

  .main { flex: 1; overflow-y: auto; padding: 24px 32px; }
  .empty-state { display: flex; flex-direction: column; align-items: center; justify-content: center; height: 100%; color: #a89e97; }
  .empty-state svg { margin-bottom: 16px; opacity: 0.4; }
  .empty-state p { font-size: 16px; }

  .churn-alert { background: linear-gradient(135deg, #fef2f2, #fee2e2); border: 1px solid #fca5a5; border-radius: 10px; padding: 16px 20px; margin-bottom: 20px; display: flex; align-items: center; gap: 12px; }
  .churn-alert .icon { font-size: 24px; flex-shrink: 0; }
  .churn-alert .text { flex: 1; }
  .churn-alert .text h4 { font-size: 14px; font-weight: 600; color: #991b1b; }
  .churn-alert .text p { font-size: 12px; color: #7f1d1d; margin-top: 2px; }
  .churn-alert .actions { display: flex; gap: 8px; flex-shrink: 0; }
  .btn { padding: 7px 14px; border-radius: 6px; font-size: 12px; font-weight: 500; cursor: pointer; border: none; transition: all 0.15s; }
  .btn-danger { background: #dc2626; color: #fff; }
  .btn-danger:hover { background: #b91c1c; }
  .btn-primary { background: #c4834a; color: #fff; }
  .btn-primary:hover { background: #b3743f; }
  .btn-outline { background: #fff; color: #2d2926; border: 1px solid #d4cfc9; }
  .btn-outline:hover { border-color: #c4834a; }

  .detail-header { display: flex; align-items: center; gap: 16px; margin-bottom: 24px; }
  .detail-header-info h2 { font-size: 22px; font-weight: 700; }
  .detail-header-info p { color: #8a8178; font-size: 14px; margin-top: 2px; }
  .detail-header-badges { display: flex; gap: 6px; margin-top: 6px; }

  .cards { display: grid; grid-template-columns: repeat(auto-fill, minmax(160px, 1fr)); gap: 12px; margin-bottom: 28px; }
  .card { background: #fff; border-radius: 10px; padding: 16px; border: 1px solid #e5e0db; }
  .card .label { font-size: 11px; color: #8a8178; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px; }
  .card .value { font-size: 22px; font-weight: 700; }
  .card .value.danger { color: #dc2626; }
  .card .value.success { color: #059669; }
  .card .value.warning { color: #d97706; }

  .section-title { font-size: 15px; font-weight: 600; margin-bottom: 12px; padding-bottom: 8px; border-bottom: 2px solid #e5e0db; display: flex; align-items: center; justify-content: space-between; }

  .info-grid { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 8px 24px; margin-bottom: 28px; background: #fff; border-radius: 10px; padding: 16px 20px; border: 1px solid #e5e0db; }
  .info-item .label { font-size: 11px; color: #8a8178; text-transform: uppercase; letter-spacing: 0.3px; }
  .info-item .val { font-size: 14px; font-weight: 500; margin-top: 2px; }

  table { width: 100%; border-collapse: collapse; background: #fff; border-radius: 10px; overflow: hidden; border: 1px solid #e5e0db; margin-bottom: 28px; }
  th { text-align: left; padding: 10px 14px; font-size: 11px; color: #8a8178; text-transform: uppercase; letter-spacing: 0.5px; background: #faf8f6; border-bottom: 1px solid #e5e0db; }
  td { padding: 10px 14px; font-size: 13px; border-bottom: 1px solid #f0ece8; }
  tr:last-child td { border-bottom: none; }
  tr:hover td { background: #faf8f6; }
  td.new-row { background: #f0fdf4 !important; }

  .modal-overlay { position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.4); display: flex; align-items: center; justify-content: center; z-index: 100; }
  .modal { background: #fff; border-radius: 12px; padding: 24px; width: 420px; max-width: 90vw; box-shadow: 0 20px 60px rgba(0,0,0,0.2); }
  .modal h3 { font-size: 18px; margin-bottom: 16px; }
  .modal .form-group { margin-bottom: 12px; }
  .modal label { display: block; font-size: 12px; font-weight: 500; color: #8a8178; margin-bottom: 4px; text-transform: uppercase; letter-spacing: 0.3px; }
  .modal select, .modal input { width: 100%; padding: 8px 12px; border: 1px solid #d4cfc9; border-radius: 6px; font-size: 14px; outline: none; }
  .modal select:focus, .modal input:focus { border-color: #c4834a; box-shadow: 0 0 0 2px rgba(196,131,74,0.15); }
  .modal .modal-actions { display: flex; gap: 8px; justify-content: flex-end; margin-top: 16px; }
  .toast { position: fixed; bottom: 24px; right: 24px; background: #2d2926; color: #fff; padding: 12px 20px; border-radius: 8px; font-size: 14px; z-index: 200; animation: slideUp 0.3s ease; }
  .toast.success { background: #059669; }
  .toast.error { background: #dc2626; }
  @keyframes slideUp { from { transform: translateY(20px); opacity: 0; } to { transform: translateY(0); opacity: 1; } }
  .loading { text-align: center; padding: 40px; color: #a89e97; }

  /* Genie Panel */
  .genie-toggle { background: #5c3d9e; color: #fff; border: none; padding: 7px 14px; border-radius: 6px; font-size: 12px; font-weight: 500; cursor: pointer; display: flex; align-items: center; gap: 6px; margin-left: 8px; }
  .genie-toggle:hover { background: #4c2d8e; }
  .genie-panel { position: fixed; right: 0; top: 0; bottom: 0; width: 440px; background: #fff; border-left: 1px solid #e5e0db; z-index: 50; display: flex; flex-direction: column; transform: translateX(100%); transition: transform 0.25s ease; box-shadow: -4px 0 20px rgba(0,0,0,0.1); }
  .genie-panel.open { transform: translateX(0); }
  .genie-header { padding: 16px 20px; background: #5c3d9e; color: #fff; display: flex; align-items: center; gap: 10px; }
  .genie-header h3 { font-size: 16px; flex: 1; }
  .genie-close { background: none; border: none; color: #fff; font-size: 20px; cursor: pointer; padding: 4px 8px; }
  .genie-messages { flex: 1; overflow-y: auto; padding: 16px; }
  .genie-msg { margin-bottom: 16px; }
  .genie-msg.user { text-align: right; }
  .genie-msg.user .bubble { background: #5c3d9e; color: #fff; display: inline-block; padding: 8px 14px; border-radius: 12px 12px 2px 12px; max-width: 85%; text-align: left; font-size: 14px; }
  .genie-msg.bot .bubble { background: #f3f0f8; color: #2d2926; padding: 12px 16px; border-radius: 2px 12px 12px 12px; max-width: 95%; font-size: 14px; line-height: 1.5; }
  .genie-msg.bot .bubble p { margin-bottom: 8px; line-height: 1.6; }
  .genie-msg.bot .bubble p:last-child { margin-bottom: 0; }
  .genie-msg.bot .bubble strong { font-weight: 600; color: #2d2926; }
  .genie-section { margin-top: 12px; }
  .genie-section-label { font-size: 10px; text-transform: uppercase; letter-spacing: 0.5px; color: #8a8178; font-weight: 600; margin-bottom: 4px; }
  .genie-msg.bot .sql-block { background: #1e1b2e; color: #c4b5fd; padding: 12px 16px; border-radius: 8px; font-family: 'SF Mono', Monaco, monospace; font-size: 12px; margin-top: 4px; overflow-x: auto; white-space: pre-wrap; word-break: break-all; line-height: 1.6; }
  .genie-msg.bot .result-table-wrap { overflow-x: auto; margin-top: 4px; border-radius: 8px; border: 1px solid #e5e0db; }
  .genie-msg.bot .result-table { width: 100%; border-collapse: collapse; font-size: 12px; }
  .genie-msg.bot .result-table th { background: #f3f0f8; padding: 8px 12px; text-align: left; border-bottom: 2px solid #d8d0e8; font-weight: 600; color: #5c3d9e; white-space: nowrap; }
  .genie-msg.bot .result-table td { padding: 7px 12px; border-bottom: 1px solid #f0ece8; white-space: nowrap; }
  .genie-msg.bot .result-table tr:hover td { background: #f9f7fc; }
  .genie-msg.bot .result-table tr:last-child td { border-bottom: none; }
  .genie-row-count { font-size: 11px; color: #8a8178; margin-top: 6px; }
  .genie-suggestions { display: flex; flex-wrap: wrap; gap: 6px; margin-bottom: 12px; padding: 0 16px; }
  .genie-suggestion { font-size: 12px; padding: 6px 12px; background: #f3f0f8; border: 1px solid #d8d0e8; border-radius: 16px; cursor: pointer; color: #5c3d9e; transition: all 0.15s; }
  .genie-suggestion:hover { background: #5c3d9e; color: #fff; }
  .genie-input-row { padding: 12px 16px; border-top: 1px solid #e5e0db; display: flex; gap: 8px; }
  .genie-input-row input { flex: 1; padding: 10px 14px; border: 1px solid #d4cfc9; border-radius: 8px; font-size: 14px; outline: none; }
  .genie-input-row input:focus { border-color: #5c3d9e; box-shadow: 0 0 0 2px rgba(92,61,158,0.15); }
  .genie-input-row button { background: #5c3d9e; color: #fff; border: none; padding: 10px 16px; border-radius: 8px; cursor: pointer; font-weight: 500; }
  .genie-input-row button:disabled { opacity: 0.5; cursor: not-allowed; }
  .genie-typing { color: #8a8178; font-style: italic; font-size: 13px; }

  .offer-history { margin-bottom: 28px; }
  .offer-item { display: flex; align-items: center; gap: 12px; padding: 10px 14px; background: #fff; border: 1px solid #e5e0db; border-radius: 8px; margin-bottom: 6px; }
  .offer-item .offer-icon { width: 32px; height: 32px; border-radius: 50%; background: #fef3c7; display: flex; align-items: center; justify-content: center; font-size: 14px; flex-shrink: 0; }
  .offer-item .offer-info { flex: 1; }
  .offer-item .offer-info .type { font-size: 13px; font-weight: 600; }
  .offer-item .offer-info .detail { font-size: 12px; color: #8a8178; }
  .offer-item .offer-date { font-size: 11px; color: #a89e97; }
</style>
</head>
<body>

<header>
  <svg width="28" height="28" viewBox="0 0 28 28" fill="none"><circle cx="14" cy="14" r="13" stroke="#c4834a" stroke-width="2"/><circle cx="10" cy="11" r="2" fill="#c4834a"/><circle cx="18" cy="11" r="2" fill="#c4834a"/><circle cx="14" cy="17" r="2" fill="#c4834a"/></svg>
  <h1>Bakehouse Cookie Co.</h1>
  <span class="subtitle">Customer 360</span>
  <span class="spacer"></span>
  <span class="pill" id="customerCount"></span>
  <button class="refresh-btn" onclick="refreshAnalytics()" id="refreshBtn">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 2v6h-6M3 12a9 9 0 0 1 15-6.7L21 8M3 22v-6h6M21 12a9 9 0 0 1-15 6.7L3 16"/></svg>
    Refresh Analytics
  </button>
  <button class="genie-toggle" onclick="toggleGenie()">
    <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-2 15l-5-5 1.41-1.41L10 14.17l7.59-7.59L19 8l-9 9z"/></svg>
    Ask Genie
  </button>
</header>

<div class="genie-panel" id="geniePanel">
  <div class="genie-header">
    <svg width="20" height="20" viewBox="0 0 24 24" fill="currentColor"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-2 15l-5-5 1.41-1.41L10 14.17l7.59-7.59L19 8l-9 9z"/></svg>
    <h3>Genie — Ask anything</h3>
    <button class="genie-close" onclick="toggleGenie()">&times;</button>
  </div>
  <div class="genie-messages" id="genieMessages"></div>
  <div class="genie-suggestions" id="genieSuggestions">
    <span class="genie-suggestion" onclick="askGenie('Which customers are most likely to churn?')">Churn risk?</span>
    <span class="genie-suggestion" onclick="askGenie('Top franchises by revenue?')">Top franchises?</span>
    <span class="genie-suggestion" onclick="askGenie('Which products generate the most revenue?')">Best products?</span>
    <span class="genie-suggestion" onclick="askGenie('Revenue trend by month')">Revenue trend?</span>
  </div>
  <div class="genie-input-row">
    <input type="text" id="genieInput" placeholder="Ask a question about your data..." onkeydown="if(event.key==='Enter')askGenie()">
    <button onclick="askGenie()" id="genieSend">Ask</button>
  </div>
</div>

<div class="container">
  <div class="sidebar">
    <div class="sidebar-controls">
      <input type="text" id="search" placeholder="Search by name, email, or city..." autocomplete="off">
      <div class="filter-row">
        <span class="filter-chip active" data-filter="all" onclick="setFilter('all')">All</span>
        <span class="filter-chip churn" data-filter="churn" onclick="setFilter('churn')">High Churn</span>
        <span class="filter-chip" data-filter="High Value" onclick="setFilter('High Value')">High Value</span>
        <span class="filter-chip" data-filter="Medium Value" onclick="setFilter('Medium Value')">Medium Value</span>
        <span class="filter-chip" data-filter="Low Value" onclick="setFilter('Low Value')">Low Value</span>
      </div>
    </div>
    <div class="customer-list" id="customerList">
      <div class="loading">Loading customers...</div>
    </div>
  </div>

  <div class="main" id="main">
    <div class="empty-state">
      <svg width="48" height="48" viewBox="0 0 48 48" fill="none"><circle cx="24" cy="24" r="22" stroke="#c4834a" stroke-width="2"/><circle cx="18" cy="20" r="3" fill="#c4834a"/><circle cx="30" cy="20" r="3" fill="#c4834a"/><circle cx="24" cy="32" r="3" fill="#c4834a"/></svg>
      <p>Select a customer to view their details</p>
    </div>
  </div>
</div>

<div id="modalContainer"></div>
<div id="toastContainer"></div>

<script>
let allCustomers = [];
let activeId = null;
let activeFilter = 'all';
let franchises = [];
let products = [];

fetch('/api/franchises').then(r=>r.json()).then(d => franchises = d);
fetch('/api/products').then(r=>r.json()).then(d => products = d);

function segmentBadge(segment) {
  if (!segment) return '';
  const s = segment.toLowerCase();
  let cls = 'badge-medium';
  if (s.includes('premium')) cls = 'badge-premium';
  else if (s.includes('high')) cls = 'badge-high';
  else if (s.includes('medium')) cls = 'badge-medium';
  else if (s.includes('low')) cls = 'badge-low';
  else if (s.includes('churn') || s.includes('risk')) cls = 'badge-churning';
  return `<span class="badge ${cls}">${segment}</span>`;
}

function churnColor(p) {
  if (p == null) return '';
  if (p > 0.5) return 'danger';
  if (p > 0.2) return 'warning';
  return 'success';
}

function fmt(n) { return n != null ? Number(n).toLocaleString('nl-NL') : '—'; }
function freshnessBadge(updated_at, source) {
  if (!updated_at) return '';
  const d = new Date(updated_at);
  const ago = Math.round((Date.now() - d.getTime()) / 1000);
  const isLive = source === 'live';
  const dotClass = isLive ? 'live' : 'synced';
  let label;
  if (ago < 60) label = 'Live · updated ' + ago + 's ago';
  else if (ago < 3600) label = (isLive ? 'Live' : 'Synced') + ' · ' + Math.round(ago/60) + 'm ago';
  else label = 'Synced · ' + d.toLocaleDateString('nl-NL', {day:'numeric',month:'short',hour:'2-digit',minute:'2-digit'});
  return `<span class="freshness"><span class="dot ${dotClass}"></span>${label}</span>`;
}
function fmtCur(n) { return n != null ? '\u20AC' + Number(n).toLocaleString('nl-NL', {minimumFractionDigits: 2, maximumFractionDigits: 2}) : '—'; }
function fmtPct(n) { return n != null ? (Number(n) * 100).toFixed(1) + '%' : '—'; }
function fmtDate(d) {
  if (!d) return '—';
  return new Date(d).toLocaleDateString('nl-NL', {day: 'numeric', month: 'short', year: 'numeric'});
}

function showToast(msg, type='success') {
  const el = document.createElement('div');
  el.className = 'toast ' + type;
  el.textContent = msg;
  document.getElementById('toastContainer').appendChild(el);
  setTimeout(() => el.remove(), 3000);
}

function setFilter(f) {
  activeFilter = f;
  document.querySelectorAll('.filter-chip').forEach(c => c.classList.toggle('active', c.dataset.filter === f));
  renderList(filterCustomers());
}

async function loadCustomers() {
  const res = await fetch('/api/customers');
  allCustomers = await res.json();
  document.getElementById('customerCount').textContent = allCustomers.length + ' customers';
  renderList(allCustomers);
}

function filterCustomers() {
  let list = allCustomers;
  const q = document.getElementById('search').value.toLowerCase();
  if (q) {
    list = list.filter(c =>
      (c.full_name || (c.first_name + ' ' + c.last_name)).toLowerCase().includes(q) ||
      (c.email_address || '').toLowerCase().includes(q) ||
      (c.city || '').toLowerCase().includes(q)
    );
  }
  if (activeFilter === 'churn') {
    list = list.filter(c => c.churn_probability != null && c.churn_probability > 0.4);
  } else if (activeFilter !== 'all') {
    list = list.filter(c => c.customer_value_segment === activeFilter);
  }
  return list;
}

function renderList(customers) {
  const el = document.getElementById('customerList');
  if (!customers.length) {
    el.innerHTML = '<div class="loading">No customers match this filter</div>';
    return;
  }
  el.innerHTML = customers.map(c => {
    const initials = ((c.first_name||'?')[0] + (c.last_name||'?')[0]).toUpperCase();
    return `<div class="customer-item ${c.id === activeId ? 'active' : ''}" onclick="selectCustomer(${c.id})">
      <div class="avatar">${initials}</div>
      <div class="customer-info">
        <h3>${c.full_name || (c.first_name + ' ' + c.last_name)}</h3>
        <p>${c.city || ''}${c.country ? ', ' + c.country : ''}</p>
      </div>
      <div class="customer-meta">
        <div class="ltv">${fmtCur(c.lifetime_value)}</div>
        ${segmentBadge(c.customer_value_segment)}
      </div>
    </div>`;
  }).join('');
}

async function selectCustomer(id) {
  activeId = id;
  renderList(filterCustomers());
  document.getElementById('main').innerHTML = '<div class="loading">Loading...</div>';

  const res = await fetch('/api/customers/' + id);
  const c = await res.json();
  renderDetail(c);
}

function renderDetail(c) {
  const initials = ((c.first_name||'?')[0] + (c.last_name||'?')[0]).toUpperCase();
  const isChurning = c.churn_probability != null && c.churn_probability > 0.4;

  const churnAlert = isChurning ? `
    <div class="churn-alert">
      <div class="icon">\u26A0\uFE0F</div>
      <div class="text">
        <h4>High Churn Risk — ${fmtPct(c.churn_probability)}</h4>
        <p>This customer hasn't purchased in ${c.days_since_last_purchase || '?'} days and is at risk of churning.</p>
      </div>
      <div class="actions">
        <button class="btn btn-danger" onclick="openRetentionModal(${c.id}, '${c.full_name || c.first_name}')">Send Retention Offer</button>
      </div>
    </div>` : '';

  const txHtml = (c.transactions || []).map(t => `<tr>
    <td>${fmtDate(t.order_date)}</td>
    <td>${t.product || '—'}</td>
    <td>${t.franchise_name || '—'}</td>
    <td>${t.franchise_city || '—'}</td>
    <td style="text-align:right">${fmt(t.quantity)}</td>
    <td style="text-align:right">${fmtCur(t.totalprice || t.totalPrice)}</td>
    <td>${t.payment_method || '—'}</td>
  </tr>`).join('');

  const offersHtml = (c.retention_offers || []).map(o => `
    <div class="offer-item">
      <div class="offer-icon">\uD83C\uDF81</div>
      <div class="offer-info">
        <div class="type">${o.offer_type}</div>
        <div class="detail">${o.offer_detail}</div>
      </div>
      <div class="offer-date">${fmtDate(o.created_at)}</div>
    </div>`).join('');

  document.getElementById('main').innerHTML = `
    ${churnAlert}

    <div class="detail-header">
      <div class="avatar lg">${initials}</div>
      <div class="detail-header-info">
        <h2>${c.full_name || (c.first_name + ' ' + c.last_name)}</h2>
        <p>${c.email_address || ''} \u00B7 ${c.city || ''}${c.state ? ', ' + c.state : ''}, ${c.country || ''}</p>
        <div class="detail-header-badges">
          ${segmentBadge(c.customer_value_segment)}
          ${segmentBadge(c.customer_connection_segment)}
          ${freshnessBadge(c.updated_at, c.source)}
        </div>
      </div>
    </div>

    <div class="cards">
      <div class="card">
        <div class="label">Lifetime Value</div>
        <div class="value">${fmtCur(c.lifetime_value)}</div>
      </div>
      <div class="card">
        <div class="label">Total Orders</div>
        <div class="value">${fmt(c.total_transactions)}</div>
      </div>
      <div class="card">
        <div class="label">Avg Order Value</div>
        <div class="value">${fmtCur(c.avg_order_value)}</div>
      </div>
      <div class="card">
        <div class="label">Churn Risk</div>
        <div class="value ${churnColor(c.churn_probability)}">${fmtPct(c.churn_probability)}</div>
      </div>
      <div class="card">
        <div class="label">Items Purchased</div>
        <div class="value">${fmt(c.total_items_purchased)}</div>
      </div>
      <div class="card">
        <div class="label">Franchises Visited</div>
        <div class="value">${fmt(c.unique_franchises_visited)}</div>
      </div>
    </div>

    <h3 class="section-title">Customer Details</h3>
    <div class="info-grid">
      <div class="info-item"><div class="label">Phone</div><div class="val">${c.phone_number || '—'}</div></div>
      <div class="info-item"><div class="label">Gender</div><div class="val">${c.gender || '—'}</div></div>
      <div class="info-item"><div class="label">Continent</div><div class="val">${c.continent || '—'}</div></div>
      <div class="info-item"><div class="label">First Purchase</div><div class="val">${fmtDate(c.first_purchase_date)}</div></div>
      <div class="info-item"><div class="label">Last Purchase</div><div class="val">${fmtDate(c.last_purchase_date)}</div></div>
      <div class="info-item"><div class="label">Days Since Last</div><div class="val">${c.days_since_last_purchase != null ? c.days_since_last_purchase + ' days' : '—'}</div></div>
    </div>

    ${offersHtml ? `<h3 class="section-title">Retention Offers Sent</h3><div class="offer-history">${offersHtml}</div>` : ''}

    <h3 class="section-title">
      Recent Transactions
      <button class="btn btn-primary" onclick="openOrderModal(${c.id}, '${c.full_name || c.first_name}')">+ Place Order</button>
    </h3>
    <table>
      <thead><tr>
        <th>Date</th><th>Product</th><th>Franchise</th><th>Location</th>
        <th style="text-align:right">Qty</th><th style="text-align:right">Total</th><th>Payment</th>
      </tr></thead>
      <tbody>${txHtml || '<tr><td colspan="7" style="text-align:center;color:#a89e97;padding:20px">No transactions yet</td></tr>'}</tbody>
    </table>
  `;
}

function openOrderModal(customerId, customerName) {
  const franchiseOpts = franchises.map(f => `<option value="${f.id}">${f.franchise_name} (${f.city})</option>`).join('');
  const productOpts = products.map(p => `<option value="${p}">${p}</option>`).join('');

  document.getElementById('modalContainer').innerHTML = `
    <div class="modal-overlay" onclick="closeModal(event)">
      <div class="modal" onclick="event.stopPropagation()">
        <h3>Place Order for ${customerName}</h3>
        <div class="form-group">
          <label>Product</label>
          <select id="orderProduct">${productOpts}</select>
        </div>
        <div class="form-group">
          <label>Franchise</label>
          <select id="orderFranchise">${franchiseOpts}</select>
        </div>
        <div class="form-group">
          <label>Quantity</label>
          <input type="number" id="orderQty" value="2" min="1" max="100">
        </div>
        <div class="form-group">
          <label>Unit Price (\u20AC)</label>
          <input type="number" id="orderPrice" value="5" min="1" max="100">
        </div>
        <div class="form-group">
          <label>Payment Method</label>
          <select id="orderPayment">
            <option>Card</option><option>Cash</option><option>Mobile</option>
          </select>
        </div>
        <div class="modal-actions">
          <button class="btn btn-outline" onclick="closeModal()">Cancel</button>
          <button class="btn btn-primary" onclick="submitOrder(${customerId})">Place Order</button>
        </div>
      </div>
    </div>`;
}

function openRetentionModal(customerId, customerName) {
  document.getElementById('modalContainer').innerHTML = `
    <div class="modal-overlay" onclick="closeModal(event)">
      <div class="modal" onclick="event.stopPropagation()">
        <h3>Send Retention Offer to ${customerName}</h3>
        <div class="form-group">
          <label>Offer Type</label>
          <select id="offerType">
            <option value="Discount">Discount</option>
            <option value="Free Shipping">Free Shipping</option>
            <option value="Loyalty Bonus">Loyalty Bonus</option>
            <option value="Personal Outreach">Personal Outreach</option>
          </select>
        </div>
        <div class="form-group">
          <label>Offer Detail</label>
          <input type="text" id="offerDetail" value="20% off your next order" placeholder="Describe the offer...">
        </div>
        <div class="modal-actions">
          <button class="btn btn-outline" onclick="closeModal()">Cancel</button>
          <button class="btn btn-danger" onclick="submitRetention(${customerId})">Send Offer</button>
        </div>
      </div>
    </div>`;
}

function closeModal(e) {
  if (!e || e.target.classList.contains('modal-overlay'))
    document.getElementById('modalContainer').innerHTML = '';
}

async function submitOrder(customerId) {
  const body = {
    customer_id: customerId,
    franchise_id: parseInt(document.getElementById('orderFranchise').value),
    product: document.getElementById('orderProduct').value,
    quantity: parseInt(document.getElementById('orderQty').value),
    unit_price: parseInt(document.getElementById('orderPrice').value),
    payment_method: document.getElementById('orderPayment').value
  };
  closeModal();
  const res = await fetch('/api/orders', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body) });
  const data = await res.json();
  if (data.success) {
    showToast(data.message, 'success');
    selectCustomer(customerId);
  } else {
    showToast(data.error || 'Failed to place order', 'error');
  }
}

async function submitRetention(customerId) {
  const body = {
    customer_id: customerId,
    offer_type: document.getElementById('offerType').value,
    offer_detail: document.getElementById('offerDetail').value
  };
  closeModal();
  const res = await fetch('/api/retention-offers', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body) });
  const data = await res.json();
  if (data.success) {
    showToast(data.message, 'success');
    selectCustomer(customerId);
  } else {
    showToast(data.error || 'Failed to send offer', 'error');
  }
}

async function refreshAnalytics() {
  const btn = document.getElementById('refreshBtn');
  btn.disabled = true;
  btn.classList.add('spinning');
  const res = await fetch('/api/refresh-analytics', { method: 'POST' });
  const data = await res.json();
  btn.disabled = false;
  btn.classList.remove('spinning');
  showToast(data.message, data.success ? 'success' : 'error');
  if (data.success) {
    await loadCustomers();
    if (activeId) selectCustomer(activeId);
  }
}

document.getElementById('search').addEventListener('input', () => renderList(filterCustomers()));
loadCustomers();

// Genie
let genieConvId = null;
let genieAsking = false;

function toggleGenie() {
  document.getElementById('geniePanel').classList.toggle('open');
  if (document.getElementById('geniePanel').classList.contains('open'))
    document.getElementById('genieInput').focus();
}

async function askGenie(q) {
  if (genieAsking) return;
  const input = document.getElementById('genieInput');
  const question = q || input.value.trim();
  if (!question) return;
  input.value = '';

  const msgs = document.getElementById('genieMessages');
  msgs.innerHTML += `<div class="genie-msg user"><div class="bubble">${question}</div></div>`;
  msgs.innerHTML += `<div class="genie-msg bot" id="genieTyping"><div class="bubble genie-typing">Thinking...</div></div>`;
  msgs.scrollTop = msgs.scrollHeight;
  document.getElementById('genieSuggestions').style.display = 'none';

  genieAsking = true;
  document.getElementById('genieSend').disabled = true;

  try {
    const endpoint = genieConvId ? '/api/genie/followup' : '/api/genie/ask';
    const body = genieConvId
      ? { conversation_id: genieConvId, question }
      : { question };
    const res = await fetch(endpoint, { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body) });
    const data = await res.json();

    const typing = document.getElementById('genieTyping');
    if (typing) typing.remove();

    if (data.conversation_id) genieConvId = data.conversation_id;

    if (data.error) {
      const errMsg = typeof data.error === 'object' ? JSON.stringify(data.error) : data.error;
      msgs.innerHTML += `<div class="genie-msg bot"><div class="bubble" style="color:#dc2626">${errMsg}</div></div>`;
    } else {
      let html = '';
      if (data.text) {
        let md = data.text
          .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
          .replace(/\*(.*?)\*/g, '<em>$1</em>')
          .replace(/\n/g, '<br>');
        html += `<div>${md}</div>`;
      }
      if (data.query_result) {
        const qr = data.query_result;
        const cols = (qr.statement_response?.manifest?.schema?.columns || []).map(c => c.name);
        const rows = qr.statement_response?.result?.data_array || [];
        if (cols.length && rows.length) {
          html += '<div class="genie-section"><div class="genie-section-label">Results</div>';
          html += '<div class="result-table-wrap"><table class="result-table"><thead><tr>' + cols.map(c => `<th>${c}</th>`).join('') + '</tr></thead><tbody>';
          const showRows = rows.slice(0, 15);
          showRows.forEach(r => { html += '<tr>' + r.map(v => `<td>${v ?? '—'}</td>`).join('') + '</tr>'; });
          html += '</tbody></table></div>';
          if (rows.length > 15) html += `<div class="genie-row-count">Showing 15 of ${rows.length} rows</div>`;
          html += '</div>';
        }
      }
      
      if (!html) html = '<p>No results returned</p>';
      msgs.innerHTML += `<div class="genie-msg bot"><div class="bubble">${html}</div></div>`;
    }
  } catch(e) {
    const typing = document.getElementById('genieTyping');
    if (typing) typing.remove();
    msgs.innerHTML += `<div class="genie-msg bot"><div class="bubble" style="color:#dc2626">Error: ${e.message}</div></div>`;
  }

  genieAsking = false;
  document.getElementById('genieSend').disabled = false;
  msgs.scrollTop = msgs.scrollHeight;
}
</script>
</body>
</html>"""


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
