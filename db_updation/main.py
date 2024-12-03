import psycopg2
import pandas as pd
from datetime import datetime

# Load the data
data = pd.read_csv('template2.csv')  # Replace with your actual file path

# Check data structure
print(data.head())
print(data.columns)

# Connect to the database
conn = psycopg2.connect(
    database="", 
        user="", 
        host="", 
        password="", 
        port= 5432
    connect_timeout=60
)

cur = conn.cursor()

try:
    # Pre-fetch IDs
    cur.execute("SELECT finding_text, id FROM bionic_data.findings WHERE template_fk = 3 AND study_fk = 1")
    findings_map = {row[0]: row[1] for row in cur.fetchall()}

    cur.execute("SELECT template_name, id FROM bionic_data.templates WHERE template_name = 'template_3'")
    templates_map = {row[0]: row[1] for row in cur.fetchall()}

    cur.execute("SELECT pathology, id FROM bionic_data.pathologies WHERE study_fk = 1")
    pathologies_map = {row[0]: row[1] for row in cur.fetchall()}

    cur.execute("SELECT id FROM bionic_data.studies WHERE name = 'usg abdomen and pelvis'")
    study_id = cur.fetchone()[0]

    # Prepare data for bulk insert
    insert_data = []
    for _, row in data.iterrows():
        finding_id = findings_map.get(row['findings'])
        if not finding_id:
            print(f"Finding not found: {row['findings']}")
            continue

        template_id = templates_map.get('template_3')
        if not template_id:
            print(f"Template not found: {row['placeholder']}")
            continue

        for column in row.index[4:]:
            pathology_id = pathologies_map.get(column)
            if not pathology_id:
                print(f"Pathology not found: {column}")
                continue

            is_present = row[column] if not pd.isna(row[column]) else 'no'
            created_at = updated_at = datetime.now()
            insert_data.append((study_id, finding_id, template_id, pathology_id, is_present, created_at, updated_at))

    # Bulk insert
    cur.executemany("""
        INSERT INTO bionic_data.findingpathologymapping (
            study_fk, finding_fk, template_fk, pathology_fk, is_present, created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, insert_data)

    conn.commit()
    print("Data successfully inserted.")

except Exception as e:
    print(f"Error inserting data: {e}")
    conn.rollback()

finally:
    if conn:
        conn.close()
        print("Database connection closed")
