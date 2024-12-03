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
    # Load the CSV file into a DataFrame
    data = pd.read_csv("template2.csv")
    pathology_list = []
except Exception as e:
    print(f"CSV not loaded: {e}")
    conn.close()
    exit()

# Create a cursor for executing queries
cur = conn.cursor()
try:
# Iterate over each row in the DataFrame
    for _, row in data.iterrows():
        # Fetch the findings and placeholder
        finding_name = row['findings']
        placeholder = row['placeholder']

        # Get the finding_id from the 'Findings' table based on the finding text
        cur.execute("SELECT id FROM bionic_data.findings WHERE finding_text = %s and template_fk =3 and study_fk=1", (finding_name,))
        finding_id_result = cur.fetchone()
        if finding_id_result:
            finding_id = finding_id_result[0]
        else:
            print(f"Finding not found: {finding_name}")
            continue

        # Get the template_id based on the placeholder
        cur.execute("SELECT id FROM bionic_data.templates WHERE template_name = 'template_3'", )
        template_id_result = cur.fetchone()
        if template_id_result:
            template_id = template_id_result[0]
        else:
            print(f"Template not found: {placeholder}")
            continue

        # Get the pathology_id for the corresponding pathology
        for column in row.index[4:]:  # Assuming columns after 'findings' and 'placeholder' are pathologies
            pathology_name = column
            cur.execute("SELECT id FROM bionic_data.pathologies WHERE pathology = %s and study_fk=1", (pathology_name,))
            pathology_id_result = cur.fetchone()
            if pathology_id_result:
                pathology_id = pathology_id_result[0]
            else:
                print(f"Pathology not found: {pathology_name}")
                continue

            # Check if the pathology is present for the finding, set to None if not present
            is_present = row[column] if not pd.isna(row[column]) else 'no'
            created_at = updated_at = datetime.now()

            # Insert the mapping into the 'findingpathologymapping' table
            cur.execute("""
                INSERT INTO bionic_data.findingpathologymapping (
                    study_fk, finding_fk, template_fk, pathology_fk, is_present, created_at, updated_at
                ) 
                VALUES (
                    (SELECT id FROM bionic_data.studies WHERE name = 'usg abdomen and pelvis'),
                    %s, %s, %s, %s, %s, %s
                )
            """, (finding_id, template_id, pathology_id, is_present, created_at, updated_at))

    # Commit all inserts at once
    conn.commit()

    print("Data successfully inserted.")

except Exception as e:
    print(f"Error inserting data: {e}")
    conn.rollback()

finally:
    # Close the database connection
    if conn:
        conn.close()
        print("Database connection closed")
