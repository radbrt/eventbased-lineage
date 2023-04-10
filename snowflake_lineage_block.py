import prefect
from prefect.blocks.core import Block
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
import pandas as pd
import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML
from pydantic import SecretBytes, SecretStr
from typing import Optional, Union



class SnowflakeLineageBlock(Block):
    """Contains a connection to Snowflake and the functions to get lineage"""

    account: str
    database: str
    db_schema: str
    user: str
    password: str
    warehouse: str
    role: str
    flow_name: Optional[str] = None

    _block_type_name = "SnowflakeLineage"
    _block_type_slug = "snowflake-lineage"


    @property
    def flow_id(self):
        try:
            return str(prefect.context.get_run_context().flow_run.id)
        except AttributeError as e:
            return str(prefect.context.get_run_context().task_run.flow_run_id)


    @property
    def flow_name(self):
        try:
            return str(prefect.context.get_run_context().flow_run.name)
        except Exception as e:
            return None


    @property
    def connection_string(self):
        return URL(
            account=self.account,
            user=self.user,
            password=self.password,
            database=self.database,
            schema=self.db_schema,
            warehouse=self.warehouse,
            role=self.role,
        )

    def extract_tables(self, parsed):
        tables = []
        from_seen = False
        for item in parsed.tokens:
            if from_seen:
                if isinstance(item, IdentifierList):
                    for identifier in item.get_identifiers():
                        tables.append(str(identifier.get_real_name()))
                elif isinstance(item, Identifier):
                    tables.append(str(item.get_real_name()))
                    from_seen = False
            elif item.ttype is Keyword and (item.value.upper() in ("FROM", "JOIN")):
                from_seen = True
        return tables


    def list_tables_selected_in_query(self, sql_query):
        parsed = sqlparse.parse(sql_query)
        for statement in parsed:
            if statement.get_type() == 'SELECT':
                tables = self.extract_tables(statement)
                return tables
        return []


    def extract_created_or_inserted_tables(self, parsed):
        tables = []
        for item in parsed.tokens:
            if item.ttype is Keyword and item.value.upper() in ("INTO", "TABLE"):
                # The next token after INTO or TABLE should be the table name
                next_item = parsed.token_next(parsed.token_index(item))[1]
                if isinstance(next_item, Identifier):
                    tables.append(str(next_item.get_real_name()))
        return tables


    def find_created_or_inserted_tables(self, sql_query):
        parsed = sqlparse.parse(sql_query)
        tables = []
        for statement in parsed:
            if statement.get_type() in ('CREATE', 'INSERT'):
                tables.extend(self.extract_created_or_inserted_tables(statement))
        return tables


    def run_query(self, query: str) -> list[dict[str, str]]:
        """Run a query against Snowflake and return the results"""

        engine = create_engine(self.connection_string)
        with engine.connect() as connection:
            result = connection.execute(query)
            lineage_event = self.make_lineage_event(query)
            print(lineage_event)

            return [dict(row) for row in result]


    def write_df(self, df, table_name):
        """Write dataframe to table"""

        engine = create_engine(self.connection_string)

        df.to_sql(table_name, con=engine, if_exists='replace', index=False)

        lineage_event = self.make_lineage_event(f"CREATE TABLE {table_name}  AS (SELECT 1 AS dummy)")
        print(lineage_event)


    def get_df_from_query(self, query):
        """Run a query against Snowflake and return the results"""

        engine = create_engine(self.connection_string)

        df = pd.read_sql(query, con=engine)

        lineage_event = self.make_lineage_event(query)
        print(lineage_event)

        return df

    @staticmethod
    def _decode_secret(secret: Union[SecretStr, SecretBytes]) -> Optional[bytes]:
        """
        Decode the provided secret into bytes. If the secret is not a
        string or bytes, or it is whitespace, then return None.
        Args:
            secret: The value to decode.
        Returns:
            The decoded secret as bytes.
        """
        if isinstance(secret, (SecretBytes, SecretStr)):
            secret = secret.get_secret_value()

        if not isinstance(secret, (bytes, str)) or len(secret) == 0 or secret.isspace():
            return None

        return secret if isinstance(secret, bytes) else secret.encode()


    def make_lineage_event(self, query):
        """Return a dictionary of the lineage event for the query"""

        tables_queried = self.list_tables_selected_in_query(query)
        tables_created = self.find_created_or_inserted_tables(query)

        full_queried_table_references = [self.get_full_tablereference(table) for table in tables_queried]
        full_created_table_references = [self.get_full_tablereference(table) for table in tables_created]

        event_record = {
            "flow_run_name": self.flow_name,
            "flow_run_id": self.flow_id,
            "inputs": [],
            "outputs": [],
        }

        for table in tables_created:
            schema = self.get_table_schema(table)
            event_record["outputs"].append({"table": table, "schema": schema})

        for table in tables_queried:
            schema = self.get_table_schema(table)
            event_record["inputs"].append({"table": table, "schema": schema})

        return event_record


    def parse_full_table_name(self, full_table_name: str) -> tuple[str | None, str | None, str]:
        """Parse a fully qualified table name into its parts."""
        db_name: str | None = None
        schema_name: str | None = None

        parts = full_table_name.split(".")
        if len(parts) == 1:
            table_name = full_table_name
        if len(parts) == 2:
            schema_name, table_name = parts
        if len(parts) == 3:
            db_name, schema_name, table_name = parts

        return db_name, schema_name, table_name


    def get_full_tablereference(self, extracted_reference: str) -> str:
        """Return the full table reference for the extracted reference"""
        db_name, schema_name, table_name = self.parse_full_table_name(extracted_reference)
        if db_name is None:
            db_name = self.database
        if schema_name is None:
            schema_name = self.db_schema

        return f"{db_name}.{schema_name}.{table_name}"


    def get_table_schema(self, table_name: str) -> dict[str, str]:
        """Return the schema for the table"""
        query = f"DESCRIBE TABLE {table_name}"

        engine = create_engine(self.connection_string)
        result = engine.execute(query)

        return [dict(row) for row in result]
