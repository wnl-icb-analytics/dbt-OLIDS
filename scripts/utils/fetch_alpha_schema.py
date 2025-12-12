"""
Fetch current schema definitions from Data_Store_OLIDS_Alpha database.
Used to align base and stable models with actual database structure.
Uses SNOWFLAKE_ROLE from .env to access the alpha database.
"""

import os
import json
from datetime import datetime
from dotenv import load_dotenv
from snowflake.snowpark import Session
import pandas as pd

# Load environment variables
load_dotenv()

# Database to query (quoted mixed case)
DATABASE = '"Data_Store_OLIDS_Alpha"'

# Create Snowflake session with SSO
connection_parameters = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "authenticator": "externalbrowser",  # SSO
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
}

print("Connecting to Snowflake...")
session = Session.builder.configs(connection_parameters).create()
print(f"Connected as {session.get_current_user()}")
print(f"Querying database: {DATABASE}\n")

# Get all schemas in the database
schemas_query = f"""
SELECT schema_name
FROM {DATABASE}.INFORMATION_SCHEMA.SCHEMATA
WHERE schema_name NOT IN ('INFORMATION_SCHEMA')
ORDER BY schema_name
"""

schemas_df = session.sql(schemas_query).to_pandas()
schemas = schemas_df['SCHEMA_NAME'].tolist()

print(f"Found {len(schemas)} schemas: {', '.join(schemas)}\n")

# Store schema information
schema_info = {}

for schema in schemas:
    print(f"{'='*80}")
    print(f"Schema: {schema}")
    print(f"{'='*80}")
    
    # Get all tables in the schema
    tables_query = f"""
    SELECT table_name, table_type
    FROM {DATABASE}.INFORMATION_SCHEMA.TABLES
    WHERE table_schema = '{schema}'
    ORDER BY table_name
    """
    
    tables_df = session.sql(tables_query).to_pandas()
    
    if tables_df.empty:
        print(f"No tables found in {schema}\n")
        continue
    
    print(f"Found {len(tables_df)} tables\n")
    
    schema_info[schema] = {}
    
    for idx, row in tables_df.iterrows():
        table_name = row['TABLE_NAME']
        table_type = row['TABLE_TYPE']
        
        print(f"  Table: {table_name} ({table_type})")
        
        # Get column information
        columns_query = f"""
        SELECT
            column_name,
            data_type,
            is_nullable,
            column_default,
            ordinal_position,
            comment
        FROM {DATABASE}.INFORMATION_SCHEMA.COLUMNS
        WHERE table_schema = '{schema}'
          AND table_name = '{table_name}'
        ORDER BY ordinal_position
        """
        
        columns_df = session.sql(columns_query).to_pandas()
        
        # Store column information
        columns = []
        for col_idx, col_row in columns_df.iterrows():
            col_info = {
                'column_name': col_row['COLUMN_NAME'],
                'data_type': col_row['DATA_TYPE'],
                'is_nullable': col_row['IS_NULLABLE'] == 'YES',
                'column_default': col_row['COLUMN_DEFAULT'],
                'ordinal_position': int(col_row['ORDINAL_POSITION']),
                'comment': col_row['COMMENT']
            }
            columns.append(col_info)
        
        schema_info[schema][table_name] = {
            'table_type': table_type,
            'columns': columns,
            'column_count': len(columns)
        }
        
        print(f"    Columns: {len(columns)}")
    
    print()

# Close session
session.close()

# Generate output files
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
output_dir = "scripts/utils/schema_outputs"
os.makedirs(output_dir, exist_ok=True)

# Save as JSON
json_file = os.path.join(output_dir, f"alpha_schema_{timestamp}.json")
with open(json_file, 'w') as f:
    json.dump(schema_info, f, indent=2, default=str)
print(f"Schema information saved to: {json_file}")

# Generate summary report
report_file = os.path.join(output_dir, f"alpha_schema_report_{timestamp}.txt")
with open(report_file, 'w') as f:
    f.write(f"Data_Store_OLIDS_Alpha Schema Report\n")
    f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    f.write(f"{'='*80}\n\n")
    
    for schema, tables in schema_info.items():
        f.write(f"Schema: {schema}\n")
        f.write(f"{'-'*80}\n")
        
        for table_name, table_info in tables.items():
            f.write(f"\n  Table: {table_name} ({table_info['table_type']})\n")
            f.write(f"    Columns ({table_info['column_count']}):\n")
            
            for col in table_info['columns']:
                nullable = "NULL" if col['is_nullable'] else "NOT NULL"
                default = f" DEFAULT {col['column_default']}" if col['column_default'] else ""
                comment = f" -- {col['comment']}" if col['comment'] else ""
                f.write(f"      {col['ordinal_position']:3d}. {col['column_name']:<40} {col['data_type']:<20} {nullable}{default}{comment}\n")
        
        f.write(f"\n")

print(f"Summary report saved to: {report_file}")

# Generate CSV for easy comparison
csv_file = os.path.join(output_dir, f"alpha_schema_{timestamp}.csv")
csv_rows = []
for schema, tables in schema_info.items():
    for table_name, table_info in tables.items():
        for col in table_info['columns']:
            csv_rows.append({
                'schema': schema,
                'table_name': table_name,
                'table_type': table_info['table_type'],
                'ordinal_position': col['ordinal_position'],
                'column_name': col['column_name'],
                'data_type': col['data_type'],
                'is_nullable': col['is_nullable'],
                'column_default': col['column_default'],
                'comment': col['comment']
            })

csv_df = pd.DataFrame(csv_rows)
csv_df.to_csv(csv_file, index=False)
print(f"CSV export saved to: {csv_file}")

print("\nDone!")

