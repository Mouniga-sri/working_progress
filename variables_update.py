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
        database="ai", 
        user="aiadmin", 
        host="ai-service.postgres.database.azure.com", 
        password="Kannausepannu@", 
        port= 5432
    )
    print("Database connection success")
except Exception as e:
    print(f"Error connecting to the database: {e}")
    exit()

try:
    data = pd.read_csv("reports.csv")
except Exception as e:
    print(f"csv not loaded : {e}")
    conn.close()
    exit()

try:
    cur = conn.cursor()
    for index, row in data.iterrows():
        pathology = row['PATHOLOGIES'] if not pd.isna(row['PATHOLOGIES']) else None
        placeholder = row['PLACEHOLDER'] if not pd.isna(row['PLACEHOLDER']) else None
        created_at = updated_at = datetime.now()

        cur.execute("""
select * from bionic_data.pathology v where v.pathology = %s and study_fk = 1 """,(pathology,))
        exist = cur.fetchone()
        
        if exist:
            # Update existing row
            cur.execute("""
                UPDATE bionic_data.pathology
                SET
                    placeholder = %s,
                    updated_at = %s
                WHERE pathology = %s and study_fk=1
            """, (placeholder, updated_at, pathology))
        else:
            cur.execute("""
                INSERT INTO bionic_data.pathology 
                (study_fk, pathology, created_at, updated_at)
                VALUES (
                    (SELECT id FROM bionic_data.studies s WHERE s.name = 'usg abdomen and pelvis'),
                    %s, %s, %s
                )""", (pathology, created_at, updated_at))

        # cur.execute("""
        #     INSERT INTO bionic_data.reports (study_fk,pathologies,placeholder,observation,impression,variables,question,created_at,updated_at)
        #     VALUES ((select id from bionic_data.studies s where s.name = 'usg abdomen and pelvis'),%s, %s, %s, %s, %s,%s,%s,%s)
        # """, (pathologies,placeholder,observation,impression,variables,question,created_at,updated_at))
        
        conn.commit()
    print("Data successfully inserted.")
except Exception as e:
    print(f"error inserting data : {e}")
    conn.rollback()
finally:
    conn.close()
    print("Database connection closed")
