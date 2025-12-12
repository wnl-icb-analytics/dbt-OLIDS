"""
Compare database schema columns with base model columns to identify missing fields.
"""

import csv
import os
import re
from pathlib import Path
from collections import defaultdict

# Read schema CSV
schema_file = "scripts/utils/schema_outputs/alpha_schema_20251212_172334.csv"
base_models_dir = "models/olids/base"

# Load schema columns by table
schema_columns = defaultdict(set)
with open(schema_file, 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        schema = row['schema']
        table = row['table_name']
        column = row['column_name']
        # Skip TEMP schemas
        if '_TEMP' not in schema:
            schema_columns[(schema, table)].add(column.upper())

# Load base model columns
model_columns = defaultdict(set)
model_files = {}

for sql_file in Path(base_models_dir).glob("*.sql"):
    if sql_file.name == "schema.yml":
        continue
    
    # Extract table name from filename
    # base_olids_observation.sql -> OBSERVATION
    model_name = sql_file.stem.replace("base_olids_", "").upper()
    
    # Read SQL file and extract SELECT columns
    with open(sql_file, 'r') as f:
        content = f.read()
        
        # Find SELECT statement
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', content, re.DOTALL | re.IGNORECASE)
        if select_match:
            select_clause = select_match.group(1)
            
            # Extract column names (handle aliases)
            # Pattern: column_name or column_name AS alias or alias.column_name
            column_pattern = r'(\w+(?:\.\w+)?)\s*(?:AS\s+(\w+))?'
            columns = re.findall(column_pattern, select_clause, re.IGNORECASE)
            
            for col_match in columns:
                col = col_match[0] if col_match[0] else col_match[1]
                # Remove table prefix if present
                col = col.split('.')[-1].upper()
                # Skip if it's a function call or subquery
                if '(' not in col and ')' not in col:
                    model_columns[model_name].add(col)
    
    model_files[model_name] = sql_file

# Map model names to schema tables
# This mapping needs to be maintained manually based on naming conventions
model_to_schema = {
    'ALLERGY_INTOLERANCE': ('OLIDS_COMMON', 'ALLERGY_INTOLERANCE'),
    'APPOINTMENT': ('OLIDS_COMMON', 'APPOINTMENT'),
    'APPOINTMENT_PRACTITIONER': ('OLIDS_COMMON', 'APPOINTMENT_PRACTITIONER'),
    'DIAGNOSTIC_ORDER': ('OLIDS_COMMON', 'DIAGNOSTIC_ORDER'),
    'ENCOUNTER': ('OLIDS_COMMON', 'ENCOUNTER'),
    'EPISODE_OF_CARE': ('OLIDS_COMMON', 'EPISODE_OF_CARE'),
    'FLAG': ('OLIDS_COMMON', 'FLAG'),
    'LOCATION': ('OLIDS_COMMON', 'LOCATION'),
    'LOCATION_CONTACT': ('OLIDS_COMMON', 'LOCATION_CONTACT'),
    'MEDICATION_ORDER': ('OLIDS_COMMON', 'MEDICATION_ORDER'),
    'MEDICATION_STATEMENT': ('OLIDS_COMMON', 'MEDICATION_STATEMENT'),
    'OBSERVATION': ('OLIDS_COMMON', 'OBSERVATION'),
    'ORGANISATION': ('OLIDS_COMMON', 'ORGANISATION'),
    'PATIENT_PERSON': ('OLIDS_COMMON', 'PATIENT_PERSON'),
    'PATIENT_REGISTERED_PRACTITIONER_IN_ROLE': ('OLIDS_COMMON', 'PATIENT_REGISTERED_PRACTITIONER_IN_ROLE'),
    'PRACTITIONER': ('OLIDS_COMMON', 'PRACTITIONER'),
    'PRACTITIONER_IN_ROLE': ('OLIDS_COMMON', 'PRACTITIONER_IN_ROLE'),
    'PROCEDURE_REQUEST': ('OLIDS_COMMON', 'PROCEDURE_REQUEST'),
    'REFERRAL_REQUEST': ('OLIDS_COMMON', 'REFERRAL_REQUEST'),
    'SCHEDULE': ('OLIDS_COMMON', 'SCHEDULE'),
    'SCHEDULE_PRACTITIONER': ('OLIDS_COMMON', 'SCHEDULE_PRACTITIONER'),
    'PATIENT': ('OLIDS_MASKED', 'PATIENT'),
    'PATIENT_ADDRESS': ('OLIDS_MASKED', 'PATIENT_ADDRESS'),
    'PATIENT_CONTACT': ('OLIDS_MASKED', 'PATIENT_CONTACT'),
    'PATIENT_UPRN': ('OLIDS_MASKED', 'PATIENT_UPRN'),
    'PERSON': ('OLIDS_MASKED', 'PERSON'),
    'CONCEPT': ('OLIDS_TERMINOLOGY', 'CONCEPT'),
    'CONCEPT_MAP': ('OLIDS_TERMINOLOGY', 'CONCEPT_MAP'),
    'POSTCODE_HASH': ('REFERENCE', 'POSTCODE_HASH'),
}

# Compare and report
print("=" * 80)
print("SCHEMA vs MODEL COLUMN COMPARISON")
print("=" * 80)
print()

missing_columns = {}
extra_columns = {}

for model_name, (schema, table) in model_to_schema.items():
    schema_cols = schema_columns.get((schema, table), set())
    model_cols = model_columns.get(model_name, set())
    
    missing = schema_cols - model_cols
    extra = model_cols - schema_cols
    
    if missing or extra:
        print(f"\n{model_name} ({schema}.{table})")
        print("-" * 80)
        
        if missing:
            missing_columns[model_name] = missing
            print(f"  MISSING COLUMNS ({len(missing)}):")
            for col in sorted(missing):
                print(f"    - {col}")
        
        if extra:
            extra_columns[model_name] = extra
            print(f"  EXTRA COLUMNS ({len(extra)}):")
            for col in sorted(extra):
                print(f"    - {col}")

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"Models with missing columns: {len(missing_columns)}")
print(f"Total missing columns: {sum(len(cols) for cols in missing_columns.values())}")
print(f"Models with extra columns: {len(extra_columns)}")
print(f"Total extra columns: {sum(len(cols) for cols in extra_columns.values())}")

