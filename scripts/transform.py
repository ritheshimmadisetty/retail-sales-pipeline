import snowflake.connector
from dotenv import load_dotenv
import os

# Load credentials from .env file
load_dotenv()

def run_transformation():
    """
    This function connects to Snowflake and runs
    the SQL transformation that moves data from
    raw.sales into transformed.sales_summary.
    """

    print("=== Starting Transformation ===\n")

    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role="SYSADMIN"
    )

    cursor = conn.cursor()

    # Activate warehouse
    cursor.execute("USE WAREHOUSE retail_wh")
    cursor.execute("USE DATABASE retail_db")
    cursor.execute("USE SCHEMA raw")

    print("Connected to Snowflake successfully.")

    # -----------------------------------------------
    # STEP 1 — Truncate the summary table
    # -----------------------------------------------
    print("Truncating existing summary data...")
    cursor.execute("TRUNCATE TABLE retail_db.transformed.sales_summary")
    print("Truncation complete.")

    # -----------------------------------------------
    # STEP 2 — Run the transformation query
    # -----------------------------------------------
    print("Running transformation query...")

    transformation_query = """
        INSERT INTO retail_db.transformed.sales_summary (
            order_date,
            category,
            city,
            total_orders,
            total_revenue,
            avg_order_value
        )
        SELECT
            order_date,
            category,
            city,
            COUNT(order_id)          AS total_orders,
            SUM(total_amount)        AS total_revenue,
            AVG(total_amount)        AS avg_order_value
        FROM retail_db.raw.sales
        WHERE status = 'Completed'
        GROUP BY order_date, category, city
        ORDER BY order_date, category, city
    """

    cursor.execute(transformation_query)
    print("Transformation query executed successfully.")

    # -----------------------------------------------
    # STEP 3 — Verify the results
    # -----------------------------------------------
    cursor.execute("SELECT COUNT(*) FROM retail_db.transformed.sales_summary")
    count = cursor.fetchone()[0]
    print(f"Total rows in sales_summary after transformation: {count}")

    # Show top 5 categories by revenue
    cursor.execute("""
        SELECT 
            category,
            SUM(total_revenue) as total_revenue,
            SUM(total_orders) as total_orders
        FROM retail_db.transformed.sales_summary
        GROUP BY category
        ORDER BY total_revenue DESC
        LIMIT 5
    """)

    print("\nTop categories by revenue:")
    print(f"{'Category':<20} {'Total Revenue':>15} {'Total Orders':>15}")
    print("-" * 50)
    for row in cursor.fetchall():
        print(f"{row[0]:<20} {row[1]:>15,.2f} {row[2]:>15}")

    # Close connection
    cursor.close()
    conn.close()
    print("\nConnection closed.")
    print("\n=== Transformation Complete ===")


if __name__ == "__main__":
    run_transformation()


