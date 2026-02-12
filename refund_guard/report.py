import os
import pandas as pd
from refund_guard.db import get_engine


def generate_report(database_url: str, reports_dir: str) -> str:
    os.makedirs(reports_dir, exist_ok=True)
    engine = get_engine(database_url)

    with engine.begin() as conn:
        kpis = pd.read_sql(
            """
            SELECT
              COUNT(*) AS total_orders,
              SUM(is_returned) AS total_returns,
              ROUND(1.0 * SUM(is_returned) / COUNT(*), 4) AS return_rate,
              SUM(tamper_reason_flag) AS tamper_claims,
              ROUND(AVG(risk_score), 2) AS avg_risk_score
            FROM fact_returns;
            """,
            conn,
        )

        top_products = pd.read_sql(
            """
            SELECT product_id, order_count, return_rate, tamper_rate, avg_risk_score, loss_exposure
            FROM product_risk_rollup
            ORDER BY tamper_rate DESC, return_rate DESC, loss_exposure DESC
            LIMIT 15;
            """,
            conn,
        )

        top_customers = pd.read_sql(
            """
            SELECT customer_id, order_count, return_rate, tamper_claim_rate, avg_risk_score, loss_exposure
            FROM customer_behavior_rollup
            ORDER BY tamper_claim_rate DESC, return_rate DESC, loss_exposure DESC
            LIMIT 15;
            """,
            conn,
        )

    out_path = os.path.join(reports_dir, "summary.md")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("# RefundGuard Summary\n\n")

        f.write("## Key metrics\n\n")
        f.write(kpis.to_markdown(index=False))
        f.write("\n\n")

        f.write("## Highest-risk products\n\n")
        f.write(top_products.to_markdown(index=False))
        f.write("\n\n")

        f.write("## Highest-risk customers\n\n")
        f.write(top_customers.to_markdown(index=False))
        f.write("\n")

    return out_path
