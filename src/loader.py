import csv
import os

from src import db
from src.models.column_metadata import ColumnMetadata
from src.models.schema_metadata import SchemaMetadata
from src.models.metadata import Metadata
from db import get_connection
from src.models.table_metadata import TableMetadata

def load_metadata(metadata_file_path: str):
    metadata: Metadata = Metadata()
    with open(metadata_file_path, 'r') as file:
        reader = csv.reader(file)
        headers = [h.strip() for h in next(reader)] # Remove leading/trailing whitespace from the headers as "TABLE_NAME" was resolving " TABLE_NAME"
        reader = csv.DictReader(file, fieldnames=headers)

        for row in reader:
            table_schema = row['TABLE_SCHEMA'].strip()
            table_name = row['TABLE_NAME'].strip()
            column_name = row['COLUMN_NAME'].strip()
            data_type = row['DATA_TYPE'].strip()

            if table_schema not in metadata.schemas:
                metadata.schemas[table_schema] = SchemaMetadata(table_schema)

            if table_name not in metadata.schemas[table_schema].tables:
                metadata.schemas[table_schema].tables[table_name] = TableMetadata(table_name)

            metadata.schemas[table_schema].tables[table_name].columns.append(ColumnMetadata(column_name, data_type))

    return metadata

def insert_data(metadata: Metadata, data_file_path: str):
    with get_connection() as conn:
        with conn.cursor() as cursor:
            for schema_name, schema in metadata.schemas.items():
                for table_name, table in schema.tables.items():

                    file_path = os.path.join(data_file_path, f"{table_name}.csv")

                    try:
                        with open(file_path, 'r') as file:
                            try:
                                reader = csv.DictReader(file)
                                csv_columns = reader.fieldnames
                            # Gracefully handle invalid files
                            except Exception as e:
                                print(f"Error processing data file for {table_name}: {e}")
                                continue

                            # Handle column additions and removals
                            validate_columns(csv_columns, table)

                            inserted_count = db.insert_data(conn, reader, cursor, schema_name, table)

                            # Prove all records in a given file were inserted successfully
                            validate_success(file, table_name, inserted_count)

                    except FileNotFoundError:
                        print(f"Data file for {table_name} not found at {file_path}. Skipping this table.")
                        continue

def validate_columns(csv_columns, table:TableMetadata):
    csv_columns = set(csv_columns)
    expected_columns = set(col.name for col in table.columns)

    # Check for missing columns
    missing_columns = expected_columns - csv_columns
    if missing_columns:
        print(f"Missing columns in {table.name} CSV file: {', '.join(missing_columns)}. These will be populated with NULL.")

    # Check for extra columns
    extra_columns = csv_columns - expected_columns
    if extra_columns:
        print(f"Extra columns in {table.name} CSV file: {', '.join(extra_columns)}. These will be ignored.")

def validate_success(file, table_name, inserted_count):
    file.seek(0)  # Reset file pointer to the beginning
    next(file) # Skip header row
    row_count = sum(1 for _ in file)
    if inserted_count == row_count:
        print(f"All {row_count} rows inserted successfully into {table_name}.")
    else:
        print(f"Warning: Only inserted {inserted_count} out of {row_count} rows into {table_name}. Check for errors.")