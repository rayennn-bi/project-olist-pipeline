import os
import sys
import psycopg2


def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "olist_dw"),
        user=os.getenv("POSTGRES_USER", "airflow"),
        password=os.getenv("POSTGRES_PASSWORD", "airflow"),
    )


def run_check(cur, name: str, query: str, expected: int = 0) -> None:
    cur.execute(query)
    result = cur.fetchone()[0]
    print(f"[{name}] Result: {result} | Expected: {expected}")
    if result != expected:
        raise ValueError(f"Quality check failed: {name} = {result}, expected {expected}")


def main() -> None:
    table_name = "fact_sales_enriched"
    print(f"--- Running Data Quality Checks for: {table_name} ---")

    conn = get_connection()
    cur = conn.cursor()

    checks = [
        (
            "negative_payment_rows",
            "SELECT COUNT(*) FROM warehouse.fact_sales_enriched WHERE total_payment_value < 0;",
            0,
        ),
        (
            "duplicate_order_item_rows",
            """
            SELECT COUNT(*) FROM (
                SELECT order_id, order_item_id
                FROM warehouse.fact_sales_enriched
                GROUP BY order_id, order_item_id
                HAVING COUNT(*) > 1
            ) t;
            """,
            0,
        ),
        (
            "mismatched_payment_count",
            "SELECT COUNT(*) FROM warehouse.fact_sales_enriched WHERE payment_count < 0;",
            0,
        )
    ]

    for name, query, expected in checks:
        run_check(cur, name, query, expected)

    cur.close()
    conn.close()
    print(f"--- All data quality checks passed for {table_name} ---")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)