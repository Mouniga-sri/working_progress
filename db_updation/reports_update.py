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
        pathologies = row['PATHOLOGIES'] if not pd.isna(row['PATHOLOGIES']) else None
        placeholder = row['PLACEHOLDER'] if not pd.isna(row['PLACEHOLDER']) else None
        observation = row['OBSERVATION'] if not pd.isna(row['OBSERVATION']) else None
        impression = row['IMPRESSION'] if not pd.isna(row['IMPRESSION']) else None
        variables = row['VARIABLES'] if not pd.isna(row['VARIABLES']) else None

        question = row['QUESTION']
        if pd.isna(question):
            question = json.dumps({})  # Empty JSON object
        else:
            # Clean up the question string by removing newlines and excess spaces
            question = question.replace('\n', '').strip()  # Remove newlines and trim extra spaces
            try:
                question = json.dumps(json.loads(question))  # Ensure it's a valid JSON string
            except json.JSONDecodeError:
                question = json.dumps({})
        updated_at = datetime.now()

        cur.execute("""
select * from bionic_data.reports r where r.pathologies = %s and study_fk = 1 """,(pathologies,))
        exist = cur.fetchone()
        
        if exist:
            # Update existing row
            cur.execute("""
                UPDATE bionic_data.reports
                SET placeholder = %s,
                    observation = %s,
                    impression = %s,
                    variables = %s,
                    question = %s,
                    updated_at = %s
                WHERE pathologies = %s
            """, (placeholder, observation, impression, variables, question, updated_at, pathologies))
        else:
            cur.execute("""
                INSERT INTO bionic_data.reports 
                (study_fk, pathologies, placeholder, observation, impression, variables, question, created_at, updated_at)
                VALUES (
                    (SELECT id FROM bionic_data.studies s WHERE s.name = 'usg abdomen and pelvis'),
                    %s, %s, %s, %s, %s, %s, %s, %s
                )""", (pathologies, placeholder, observation, impression, variables, question, created_at, updated_at))

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
