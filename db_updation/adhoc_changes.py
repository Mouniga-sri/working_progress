import psycopg2
from dotenv import load_dotenv
import os
import pandas as pd
from datetime import datetime
import json

# Load environment variables
load_dotenv()

# Database connection
try:
    conn = psycopg2.connect(
        database="", 
        user="", 
        host="", 
        password="", 
        port= 5432
    )
    print("Database connection success")
except Exception as e:
    print(f"Error connecting to the database: {e}")
    exit()

# try:
#     # Load the CSV file into a DataFrame
#     # data = pd.read_csv("template_1.csv")
#     pathology_list = []
# except Exception as e:
#     print(f"CSV not loaded: {e}")
#     conn.close()
#     exit()

# Create a cursor for executing queries
cur = conn.cursor()

# UPDATE THE NULL VALUES IN FINDINGPATHOLOGYMAPPING
try:
    cur.execute(""" 
        update bionic_data.findingpathologymapping
        set is_present = 'no' where is_present is NULL
    """)
    conn.commit()
    print("Data successfully updated.")
except Exception as e:
    print(f"Error inserting data: {e}")
    conn.rollback()

finally:
    # Close the database connection
    if conn:
        conn.close()
        print("Database connection closed")