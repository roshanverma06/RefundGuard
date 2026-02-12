import sqlite3


def run_checks(db_path: str = "refund_guard.db") -> dict:
    issues: dict = {}

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = {r[0] for r in cur.fetchall()}

    required_tables = {
        "stg_returns",
        "fact_returns",
        "seller_risk_rollup",
        "customer_behavior_rollup",
    }
    missing_tables = sorted(list(required_tables - tables))
    if missing_tables:
        issues["missing_tables"] = missing_tables
        conn.close()
        return issues

    for t in required_tables:
        cur.execute(f"SELECT COUNT(*) FROM {t};")
        count = cur.fetchone()[0]
        if count == 0:
            issues[f"empty_{t}"] = True

    cur.execute("SELECT COUNT(*) FROM fact_returns WHERE order_id IS NULL;")
    if cur.fetchone()[0] > 0:
        issues["null_order_id"] = True

    cur.execute(
        """
        SELECT COUNT(*)
        FROM (
            SELECT order_id
            FROM fact_returns
            GROUP BY order_id
            HAVING COUNT(*) > 1
        );
        """
    )
    if cur.fetchone()[0] > 0:
        issues["duplicate_order_id"] = True

    cur.execute("SELECT MIN(risk_score), MAX(risk_score) FROM fact_returns;")
    mn, mx = cur.fetchone()
    if mn is None or mx is None:
        issues["risk_score_missing"] = True
    else:
        if mn < 0 or mx > 100:
            issues["risk_score_out_of_range"] = {"min": mn, "max": mx}

    conn.close()
    return issues
