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

try:
    data = pd.read_csv("template2.csv")
except Exception as e:
    print(f"csv not loaded : {e}")
    conn.close()
    exit()

try:
    cur = conn.cursor()
    template_name = "template_3"
    created_at=updated_at = datetime.now()

    cur.execute("""
        INSERT INTO bionic_data.templates (study_fk,template_name,created_at,updated_at)
        VALUES ((select id from bionic_data.studies s where s.name = 'usg abdomen and pelvis'),%s, %s, %s)
    """, (template_name,created_at,updated_at,))
        
    conn.commit()

    print("Data successfully inserted.")

except Exception as e:
    print(f"error inserting data : {e}")
    conn.rollback()

finally:
    conn.close()
    print("Database connection closed")
