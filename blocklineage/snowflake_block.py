import datetime
import json
from typing import Any, Dict, List, Optional, Tuple, Type, Union

import pandas as pd
from pandas.io.sql import has_table
import prefect
import requests
import sqlparse
from prefect.blocks.core import Block
from pydantic import SecretBytes, SecretStr
from snowflake.connector.connection import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from sqlparse.sql import Identifier, IdentifierList
from sqlparse.tokens import DML, Keyword
from prefect.events import emit_event
from blocklineage.core import LineageBlock
from blocklineage.utils import DataOperations
from sqlglot import parse_one, exp

class SnowflakeLineageBlock(LineageBlock):
    """Contains a connection to Snowflake and the functions to get lineage"""

    account: str
    database: str
    db_schema: str
    user: str
    password: SecretStr
    warehouse: str
    role: str
    _engine: Optional[Any] = None

    # def __init__(
    #     self,
    #     account: str,
    #     database: str,
    #     db_schema: str,
    #     user: str,
    #     password: SecretStr,
    #     warehouse: str,
    #     role: str,
    #     **kwargs: Any,
    # ):
    #     super().__init__(**kwargs)
    #     self.account = account
    #     self.database = database
    #     self.db_schema = db_schema
    #     self.user = user
    #     self.password = password
    #     self.warehouse = warehouse
    #     self.role = role
    #     self._engine = None
    #     self._connection = None
    #     self._unique_cursors = None

    _connection: Optional[SnowflakeConnection] = None
    _block_type_name = "SnowflakeLineage"
    _block_type_slug = "snowflake-lineage"


    @property
    def default_namespace(self):
        return f"snowflake://{self.account}"

    @property
    def prefect_resource_id(self):
        return f"lineage.snowflake"

    def connect(self):
        return self.engine.connect()


    @property
    def engine(self):
        if self._engine:
            return self._engine
        else:
            self._engine = create_engine(self.connection_string)
            return self._engine

    @property
    def connection(self):
        """Return a connection to Snowflake."""
        if self._connection:
            return self._connection
        else:
            self._connection = self.engine.connect()
            return self._connection


    @property
    def connection_string(self):
        return URL(
            account=self.account,
            user=self.user,
            password=self.password.get_secret_value(),
            database=self.database,
            schema=self.db_schema,
            warehouse=self.warehouse,
            role=self.role,
        )


    def _extract_tables(self, parsed):
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


    def _parse_full_table_name(
        self, full_table_name: str
    ):
        """Parse a fully qualified table name into its parts."""
        db_name = None
        schema_name = None

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
        db_name, schema_name, table_name = self._parse_full_table_name(
            extracted_reference
        )
        if db_name is None:
            db_name = self.database
        if schema_name is None:
            schema_name = self.db_schema

        return f"{db_name}.{schema_name}.{table_name}"


    def _get_table_schema(self, table_name: str) -> dict[str, str]:
        """Return the schema for the table"""
        query = f"DESCRIBE TABLE {table_name}"

        result = self.engine.execute(query)
        return [{"name": row["name"], "type": row["type"]} for row in result]


    def _list_tables_selected_in_query(self, sql_query):
        parsed = parse_one(sql_query)
        for e in parsed.find_all(exp.Select):
            for tbl in e.find_all(exp.Table):
                yield f"{tbl.catalog or self.database}.{tbl.db or self.schema}.{tbl.name}"


    def _list_tables_created_in_query(self, sql_query):
        parsed = parse_one(sql_query)
        for tbl in parsed.find_all(exp.Create):
            yield f"{tbl.this.catalog or self.database}.{tbl.this.db or self.schema}.{tbl.this.name}"

    def _list_tables_inserted_in_query(self, sql_query):
        parsed = parse_one(sql_query)
        for tbl in parsed.find_all(exp.Insert):
            yield f"{tbl.this.catalog or self.database}.{tbl.db or self.schema}.{tbl.name}"



    def _start_connection(self):
        """
        Starts Snowflake database connection.
        """
        self._get_connection()
        if self._unique_cursors is None:
            self._unique_cursors = {}


    def _get_connection(self, **connect_kwargs: Dict[str, Any]) -> SnowflakeConnection:
        """
        Returns a Snowflake connection object.
        """
        if self.connection is not None:
            return self.connection

        connect_params = {
            "database": self.database,
            "warehouse": self.warehouse,
            "schema": self.db_schema,
        }
        connection = self.credentials.get_client(**connect_kwargs, **connect_params)
        self.connection = connection
        # self.logger.info("Started a new connection to Snowflake.")
        return connection


    def execute(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = None,
        cursor_type: Type[SnowflakeCursor] = SnowflakeCursor,
        **execute_kwargs: Dict[str, Any],
    ) -> None:

        if parameters:
            r = self.connection.execute(operation, parameters)
        else:
            r = self.connection.execute(operation)

        for table in self._list_tables_selected_in_query(operation):
            full_table_name = self.get_full_tablereference(table)
            table_schema = self._get_table_schema(full_table_name)
            full_uri = f"{self.default_namespace}/{full_table_name.replace('.', '/')}"
            self.emit_lineage_to_prefect(full_uri, DataOperations.READ, table_schema)

        for table in self._list_tables_created_in_query(operation):
            full_table_name = self.get_full_tablereference(table)
            table_schema = self._get_table_schema(full_table_name)
            full_uri = f"{self.default_namespace}/{full_table_name.replace('.', '/')}"
            self.emit_lineage_to_prefect(full_uri, DataOperations.CREATE, table_schema)

        for table in self._list_tables_inserted_in_query(operation):
            full_table_name = self.get_full_tablereference(table)
            table_schema = self._get_table_schema(full_table_name)
            full_uri = f"{self.default_namespace}/{full_table_name.replace('.', '/')}"
            self.emit_lineage_to_prefect(full_uri, DataOperations.WRITE, table_schema)

        return r


    def read_sql(self, sql, **kwargs):
        """
        Reads a SQL query into a DataFrame and emits lineage
        We assume this is a select, and does not somehow write/drop data as well
        """
        if has_table(sql, self.connection):
            full_table_name = self.get_full_tablereference(sql)
            table_schema = self._get_table_schema(full_table_name)
            full_uri = f"{self.default_namespace}/{full_table_name.replace('.', '/')}"
            self.emit_lineage_to_prefect(full_uri, DataOperations.READ, table_schema)

        else:
            for table in self._list_tables_selected_in_query(sql):
                full_table_name = self.get_full_tablereference(table)
                table_schema = self._get_table_schema(full_table_name)
                full_uri = f"{self.default_namespace}/{full_table_name.replace('.', '/')}"
                self.emit_lineage_to_prefect(full_uri, DataOperations.READ, table_schema)

        return pd.read_sql(sql, con=self.connection, **kwargs)
