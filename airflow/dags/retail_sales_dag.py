from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add scripts folder to path so Airflow can find our scripts
sys.path.insert(0, '/opt/airflow/scripts')

# Import our functions from existing scripts
from generate_and_load import generate_sales_data, save_to_csv, load_to_snowflake
from transform import run_transformation

# -----------------------------------------------
# DEFAULT ARGUMENTS
# These apply to every task in the DAG
# -----------------------------------------------
default_args = {
    'owner': 'rithesh',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

# -----------------------------------------------
# TASK FUNCTIONS
# Each function is one task in the pipeline
# -----------------------------------------------

def task_generate_and_load():
    """
    Task 1 — Generate 1000 sales records and load into Snowflake
    """
    print("Starting data generation and load...")
    df = generate_sales_data(1000)
    save_to_csv(df, '/opt/airflow/data/sales_data.csv')
    load_to_snowflake(df)
    print("Data generation and load complete.")

def task_transform():
    """
    Task 2 — Transform raw data into summary table
    """
    print("Starting transformation...")
    run_transformation()
    print("Transformation complete.")

def task_verify():
    """
    Task 3 — Verify the pipeline ran correctly
    """
    import snowflake.connector
    from dotenv import load_dotenv
    
    load_dotenv()
    
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
    cursor.execute("USE WAREHOUSE retail_wh")
    
    # Check raw table
    cursor.execute("SELECT COUNT(*) FROM retail_db.raw.sales")
    raw_count = cursor.fetchone()[0]
    print(f"Raw table count: {raw_count}")
    
    # Check transformed table
    cursor.execute("SELECT COUNT(*) FROM retail_db.transformed.sales_summary")
    transformed_count = cursor.fetchone()[0]
    print(f"Transformed table count: {transformed_count}")
    
    cursor.close()
    conn.close()
    
    print("Verification complete. Pipeline ran successfully.")

# -----------------------------------------------
# DAG DEFINITION
# -----------------------------------------------
with DAG(
    dag_id='retail_sales_pipeline',
    default_args=default_args,
    description='End to end retail sales data pipeline',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['retail', 'snowflake', 'etl']
) as dag:

    # Define tasks
    generate_and_load_task = PythonOperator(
        task_id='generate_and_load',
        python_callable=task_generate_and_load
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=task_transform
    )

    verify_task = PythonOperator(
        task_id='verify',
        python_callable=task_verify
    )

    # Define task order
    # >> means "then run"
    generate_and_load_task >> transform_task >> verify_task