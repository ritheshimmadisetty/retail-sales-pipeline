import snowflake.connector
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Read credentials from .env file
user = os.getenv("SNOWFLAKE_USER")
password = os.getenv("SNOWFLAKE_PASSWORD")
account = os.getenv("SNOWFLAKE_ACCOUNT")
warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
database = os.getenv("SNOWFLAKE_DATABASE")
schema = os.getenv("SNOWFLAKE_SCHEMA")

# Establish connection
try:
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema
    )
    print("Connection successful!")
    
    # Run a simple test query
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_VERSION()")
    row = cursor.fetchone()
    print(f"Snowflake version: {row[0]}")
    
    cursor.close()
    conn.close()
    print("Connection closed cleanly.")

except Exception as e:
    print(f"Connection failed: {e}")

## Understanding the code line by line:

# **`from dotenv import load_dotenv`** — imports the dotenv library which knows how to read `.env` files

# **`load_dotenv()`** — this actually reads your `.env` file and loads all the variables into memory

# **`os.getenv("SNOWFLAKE_USER")`** — fetches the value of `SNOWFLAKE_USER` from the loaded environment variables. This way your password is never written directly in the code

# **`snowflake.connector.connect(...)`** — this is the actual connection to Snowflake using all the credentials

# **`try/except`** — if connection succeeds it runs the code inside `try`, if it fails it catches the error and prints it. This is called **error handling** and is a must in production code

# **`SELECT CURRENT_VERSION()`** — a simple Snowflake query that returns the Snowflake version. We use this just to confirm the connection is working

# **`cursor`** — think of a cursor like a pointer that executes queries and fetches results

# **`conn.close()`** — always close the connection after you're done. Leaving connections open wastes resources

# ---

# ## Running the script

# Go to your Command Prompt, make sure you are in your project folder with venv activated:
# ```
# cd C:\Users\Rithesh\Documents\data_project1\retail-sales-pipeline
# venv\Scripts\activate
# ```

# Now run the script:
# ```
# python scripts/test_connection.py
# ```

# You should see:
# ```
# Connection successful!
# Snowflake version: 8.x.x
# Connection closed cleanly.