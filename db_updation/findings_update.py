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
    for index, row in data.iterrows():
        finding_text = row['findings'] if not pd.isna(row['findings']) else None
        placeholder = row['placeholder'] if not pd.isna(row['placeholder']) else None
        created_at = updated_at = datetime.now()

        cur.execute("""
select * from bionic_data.findings f where f.finding_text = %s and f.study_fk = 1 and f.template_fk = 3 """,(finding_text,))
        exist = cur.fetchone()
        
        if exist:
            # Update existing row
            cur.execute("""
                UPDATE bionic_data.template
                SET
                    placeholder = %s,
                    updated_at = %s
                WHERE finding_text = %s and study_fk=1 and template_fk =3
            """, (placeholder, updated_at, finding_text))
        else:
            cur.execute("""
                INSERT INTO bionic_data.findings 
                (template_fk, study_fk, finding_text, placeholder, created_at, updated_at)
                VALUES (
                    (select id from bionic_data.templates t where t.template_name = 'template_3' and t.study_fk = 1),
                    (SELECT id FROM bionic_data.studies s WHERE s.name = 'usg abdomen and pelvis'),
                    %s, %s, %s,%s
                )""", (finding_text,placeholder, created_at, updated_at))
        conn.commit()
    print("Data successfully inserted.")
except Exception as e:
    print(f"error inserting data : {e}")
    conn.rollback()
finally:
    conn.close()
    print("Database connection closed")
