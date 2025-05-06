import csv

import psycopg2
import psycopg2.extras
from config import settings
from src.models.checking_account import CheckingAccount
from src.models.loan_account import LoanAccount
from src.models.metadata import Metadata
from src.models.table_metadata import TableMetadata
from psycopg2.extensions import connection as PsycopgConnection, cursor as PsycopgCursor

DEFAULT_SCHEMA ="dbo"
def get_connection():
    return psycopg2.connect(
        host=settings.db_host,
        port=settings.db_port,
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
    )

def create_tables(metadata: Metadata):
    with get_connection() as conn:
        with conn.cursor() as cursor:
            for schema_name, schema in metadata.schemas.items():
                create_schema_query = f'CREATE SCHEMA IF NOT EXISTS "{schema_name}";'
                cursor.execute(create_schema_query)

                for table_name, table in schema.tables.items():
                    column_definitions = ', '.join([f'"{col.name}" {col.data_type}' for col in table.columns])
                    qualified_table_name = f'"{schema_name}"."{table_name}"'
                    create_table_query = f'CREATE TABLE IF NOT EXISTS {qualified_table_name} ({column_definitions})'
                    cursor.execute(create_table_query)

            conn.commit()

def insert_data(conn: PsycopgConnection, reader: csv.DictReader, cursor: PsycopgConnection, schema_name:str, table: TableMetadata) -> int:
    inserted_count = 0
    for row in reader:
        # Set missing column values to NULL
        # With DictReader, name based access is utilized thus potential column transposition is handled
        values = [row.get(col.name, None) for col in table.columns]
        columns_str = ', '.join(f'"{col.name}"' for col in table.columns)
        placeholders = ', '.join(['%s'] * len(table.columns))
        insert_query = f'INSERT INTO "{schema_name}"."{table.name}" ({columns_str}) VALUES ({placeholders});'

        try:
            cursor.execute(insert_query, values)
            inserted_count += 1
        except Exception as e:
            print(f"Insert failed for row: {row} with error: {e}")
    conn.commit()
    return inserted_count

def get_checking_accounts() -> list[CheckingAccount]:
    query = f"""
        SELECT 
            c."ACCOUNT_GUID" AS checking_account_guid,
            c."STARTING_BALANCE",
            t."TRANSACTION_AMOUNT",
            t."POST_DATE",
            m."FIRST_NAME",
            m."LAST_NAME"
        FROM {DEFAULT_SCHEMA}."CHECKING" c
        LEFT JOIN {DEFAULT_SCHEMA}."TRANSACTIONS" t ON c."ACCOUNT_GUID" = t."ACCOUNT_GUID"
        JOIN {DEFAULT_SCHEMA}."ACCOUNTS" a ON c."ACCOUNT_GUID" = a."ACCOUNT_GUID"
        JOIN {DEFAULT_SCHEMA}."MEMBERS" m ON a."MEMBER_GUID" = m."MEMBER_GUID"
        ORDER BY c."ACCOUNT_GUID", t."POST_DATE";
    """

    accounts_map: dict[str, CheckingAccount] = {}

    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(query)
            for row in cursor.fetchall():
                guid = row["checking_account_guid"]

                if guid not in accounts_map:
                    accounts_map[guid] = CheckingAccount(
                        account_guid = guid,
                        starting_balance = row["STARTING_BALANCE"],
                        first_name = row["FIRST_NAME"],
                        last_name = row["LAST_NAME"],
                    )

                if row["TRANSACTION_AMOUNT"] is not None:
                    accounts_map[guid].transactions.append(row["TRANSACTION_AMOUNT"])

    return list(accounts_map.values())

def get_loan_accounts() -> list[LoanAccount]:
    query = f"""
        SELECT 
            l."ACCOUNT_GUID" AS loan_account_guid,
            l."STARTING_DEBT",
            t."TRANSACTION_AMOUNT",
            t."POST_DATE",
            m."FIRST_NAME",
            m."LAST_NAME"
        FROM {DEFAULT_SCHEMA}."LOANS" l
        LEFT JOIN {DEFAULT_SCHEMA}."TRANSACTIONS" t ON l."ACCOUNT_GUID" = t."ACCOUNT_GUID"
        JOIN {DEFAULT_SCHEMA}."ACCOUNTS" a ON l."ACCOUNT_GUID" = a."ACCOUNT_GUID"
        JOIN {DEFAULT_SCHEMA}."MEMBERS" m ON a."MEMBER_GUID" = m."MEMBER_GUID"
        ORDER BY l."ACCOUNT_GUID", t."POST_DATE";
    """
    accounts_map: dict[str, CheckingAccount] = {}

    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(query)
            for row in cursor.fetchall():
                guid = row["loan_account_guid"]

                if guid not in accounts_map:
                    accounts_map[guid] = LoanAccount(
                        account_guid=guid,
                        starting_debt=row["STARTING_DEBT"],
                        first_name=row["FIRST_NAME"],
                        last_name=row["LAST_NAME"],
                    )

                if row["TRANSACTION_AMOUNT"] is not None:
                    accounts_map[guid].transactions.append(row["TRANSACTION_AMOUNT"])

    return list(accounts_map.values())
