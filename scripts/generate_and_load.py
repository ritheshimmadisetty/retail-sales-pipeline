import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta
import snowflake.connector
from dotenv import load_dotenv
import os
import csv

# Initialize Faker
fake = Faker('en_IN')

# Load environment variables
load_dotenv()

# -----------------------------------------------
# STEP 1 — DEFINE REALISTIC RETAIL DATA
# -----------------------------------------------

# These are the product categories and products a real retail store would sell
products = {
    'Electronics': [
        ('Samsung Galaxy S23', 'PRD001', 45000),
        ('Apple iPhone 14', 'PRD002', 75000),
        ('Sony Headphones', 'PRD003', 8000),
        ('Dell Laptop', 'PRD004', 55000),
        ('LG Smart TV', 'PRD005', 35000)
    ],
    'Clothing': [
        ('Levis Jeans', 'PRD006', 2500),
        ('Nike T-Shirt', 'PRD007', 1500),
        ('Zara Dress', 'PRD008', 3500),
        ('Adidas Jacket', 'PRD009', 4500),
        ('Puma Shorts', 'PRD010', 1200)
    ],
    'Groceries': [
        ('Tata Salt 1kg', 'PRD011', 25),
        ('Amul Butter 500g', 'PRD012', 280),
        ('Fortune Rice 5kg', 'PRD013', 350),
        ('Aashirvaad Atta 10kg', 'PRD014', 450),
        ('Nestle Maggi 12pack', 'PRD015', 180)
    ],
    'Furniture': [
        ('Wooden Study Table', 'PRD016', 8500),
        ('Office Chair', 'PRD017', 6500),
        ('Bookshelf', 'PRD018', 4500),
        ('Sofa 3 Seater', 'PRD019', 25000),
        ('King Size Bed', 'PRD020', 35000)
    ],
    'Sports': [
        ('Cricket Bat', 'PRD021', 2500),
        ('Football', 'PRD022', 800),
        ('Yoga Mat', 'PRD023', 600),
        ('Badminton Racket', 'PRD024', 1200),
        ('Cycling Helmet', 'PRD025', 1800)
    ]
}

# Indian cities for realistic data
cities = [
    'Mumbai', 'Delhi', 'Bangalore', 'Hyderabad', 'Chennai',
    'Pune', 'Kolkata', 'Ahmedabad', 'Jaipur', 'Surat'
]

# Payment methods used in India
payment_methods = ['UPI', 'Credit Card', 'Debit Card', 'Net Banking', 'Cash on Delivery']

# Order statuses
statuses = ['Completed', 'Completed', 'Completed', 'Pending', 'Cancelled']
# Completed appears 3 times so it is more likely to be picked — this makes data realistic

# -----------------------------------------------
# STEP 2 — GENERATE 1000 SALES RECORDS
# -----------------------------------------------

def generate_sales_data(num_records=1000):
    """
    This function generates num_records number of fake sales transactions.
    It returns a pandas DataFrame.
    """
    
    records = []  # empty list to store each record
    
    # Generate a date range for the last 6 months
    # This gives us realistic spread of dates
    end_date = datetime.now()
    start_date = end_date - timedelta(days=180)
    
    for i in range(1, num_records + 1):
        
        # Pick a random category
        category = random.choice(list(products.keys()))
        
        # Pick a random product from that category
        product_name, product_id, unit_price = random.choice(products[category])
        
        # Generate random quantity between 1 and 5
        quantity = random.randint(1, 5)
        
        # Calculate total amount
        total_amount = quantity * unit_price
        
        # Generate a random date within last 6 months
        random_days = random.randint(0, 180)
        order_date = (start_date + timedelta(days=random_days)).strftime('%Y-%m-%d')
        
        # Build the record as a dictionary
        record = {
            'order_id': f'ORD{str(i).zfill(5)}',  # ORD00001, ORD00002 etc
            'customer_id': f'CUST{str(random.randint(1, 500)).zfill(4)}',  # CUST0001 to CUST0500
            'product_id': product_id,
            'product_name': product_name,
            'category': category,
            'quantity': quantity,
            'unit_price': unit_price,
            'total_amount': total_amount,
            'order_date': order_date,
            'city': random.choice(cities),
            'country': 'India',
            'payment_method': random.choice(payment_methods),
            'status': random.choice(statuses)
        }
        
        records.append(record)  # add this record to our list
    
    # Convert list of dictionaries to a pandas DataFrame
    df = pd.DataFrame(records)
    return df

# -----------------------------------------------
# STEP 3 — SAVE DATA TO CSV
# -----------------------------------------------

def save_to_csv(df, filepath):
    """
    Saves the DataFrame to a CSV file.
    CSV = Comma Separated Values, a simple file format for storing tabular data.
    """
    df.to_csv(filepath, index=False)
    print(f"Data saved to {filepath}")
    print(f"Total records generated: {len(df)}")
    print(f"Columns: {list(df.columns)}")
    print("\nSample of first 3 records:")
    print(df.head(3))

# -----------------------------------------------
# STEP 4 — LOAD DATA INTO SNOWFLAKE
# -----------------------------------------------

def load_to_snowflake(df):
    """
    This function connects to Snowflake and loads 
    the DataFrame records into the raw.sales table.
    """
    
    print("\nConnecting to Snowflake...")
    
    # Connect to Snowflake using credentials from .env file
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role ="SYSADMIN"
    )
    
    cursor = conn.cursor()

    # Explicitly activate the warehouse
    cursor.execute("USE WAREHOUSE retail_wh")
    cursor.execute("USE DATABASE retail_db")
    cursor.execute("USE SCHEMA raw")

    print("Connected. Loading data into Snowflake...")
    
    
    # This is the INSERT statement we will use to insert each row
    insert_query = """
        INSERT INTO retail_db.raw.sales (
            order_id, customer_id, product_id, product_name,
            category, quantity, unit_price, total_amount,
            order_date, city, country, payment_method, status
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """
    
    # Convert DataFrame rows to a list of tuples
    # Each tuple is one row of data
    records = []
    for _, row in df.iterrows():
        records.append((
            row['order_id'],
            row['customer_id'],
            row['product_id'],
            row['product_name'],
            row['category'],
            int(row['quantity']),
            float(row['unit_price']),
            float(row['total_amount']),
            row['order_date'],
            row['city'],
            row['country'],
            row['payment_method'],
            row['status']
        ))
    
    # executemany inserts all records in one go — much faster than inserting one by one
    cursor.executemany(insert_query, records)
    
    # commit means — make these changes permanent in the database
    conn.commit()
    
    print(f"Successfully loaded {len(df)} records into retail_db.raw.sales")
    
    # Verify by running a count query
    cursor.execute("SELECT COUNT(*) FROM retail_db.raw.sales")
    count = cursor.fetchone()[0]
    print(f"Total records now in Snowflake table: {count}")
    
    cursor.close()
    conn.close()
    print("Connection closed.")

# -----------------------------------------------
# MAIN — Run everything
# -----------------------------------------------

if __name__ == "__main__":
    
    print("=== Retail Sales Data Pipeline ===\n")
    
    # Step 1 - Generate data
    print("Generating 1000 sales records...")
    df = generate_sales_data(1000)
    
    # Step 2 - Save to CSV
    csv_path = "data/sales_data.csv"
    save_to_csv(df, csv_path)
    
    # Step 3 - Load to Snowflake
    load_to_snowflake(df)
    
    print("\n=== Pipeline completed successfully ===")