import datetime
import json
from typing import Any, Dict, List, Optional, Tuple, Type, Union

import pandas as pd
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


class SnowflakeLineageBlock(Block):
    """Contains a connection to Snowflake and the functions to get lineage"""

    account: str
    database: str
    db_schema: str
    user: str
    password: SecretStr
    warehouse: str
    role: str
    backend: Optional[str] = "marquez"
    marquez_endpoint: Optional[str] = None
    marquez_api_token: Optional[SecretStr] = None
    marquez_namespace: Optional[str] = None

    _block_type_name = "SnowflakeLineage"
    _block_type_slug = "snowflake-lineage"

    @property
    def flow_run_id(self):
        return prefect.runtime.flow_run.id

    @property
    def flow_run_name(self):
        return prefect.runtime.flow_run.name

    @property
    def flow_name(self):
        return prefect.runtime.flow_run.flow_name

    @property
    def default_namespace(self):
        return f"snowflake://{self.account}/{self.database}/{self.db_schema}"

    @property
    def connection(self):
        return create_engine(self.connection_string).connect()

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

    def _execute(self, cursor: SnowflakeCursor, inputs: Dict[str, Any]):
        """Helper method to execute operations asynchronously."""
        response = cursor.execute(**inputs)
        self.logger.info(f"Executing the operation, {inputs['command']!r}; ")

        return response

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

    def _list_tables_selected_in_query(self, sql_query):
        parsed = sqlparse.parse(sql_query)
        for statement in parsed:
            if statement.get_type() == "SELECT":
                tables = self._extract_tables(statement)
                return tables
        return []

    def _extract_created_or_inserted_tables(self, parsed):
        tables = []
        for item in parsed.tokens:
            if item.ttype is Keyword and item.value.upper() in ("INTO", "TABLE"):
                # The next token after INTO or TABLE should be the table name
                next_item = parsed.token_next(parsed.token_index(item))[1]
                if isinstance(next_item, Identifier):
                    tables.append(str(next_item.get_real_name()))
        return tables

    def _find_created_or_inserted_tables(self, sql_query):
        parsed = sqlparse.parse(sql_query)
        tables = []
        for statement in parsed:
            if statement.get_type() in ("CREATE", "INSERT"):
                tables.extend(self._extract_created_or_inserted_tables(statement))
        return tables

    def _start_connection(self):
        """
        Starts Snowflake database connection.
        """
        self.get_connection()
        if self._unique_cursors is None:
            self._unique_cursors = {}

    def get_connection(self, **connect_kwargs: Dict[str, Any]) -> SnowflakeConnection:
        """
        Returns a Snowflake connection object.
        """
        if self._connection is not None:
            return self._connection

        connect_params = {
            "database": self.database,
            "warehouse": self.warehouse,
            "schema": self.schema_,
        }
        connection = self.credentials.get_client(**connect_kwargs, **connect_params)
        self._connection = connection
        self.logger.info("Started a new connection to Snowflake.")
        return connection

    def fetch_one(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = None,
        cursor_type: Type[SnowflakeCursor] = SnowflakeCursor,
        **execute_kwargs: Dict[str, Any],
    ) -> Tuple[Any]:

        inputs = dict(
            command=operation,
            params=parameters,
            **execute_kwargs,
        )

        new, cursor = self._get_cursor(inputs, cursor_type=cursor_type)
        if new:
            self._execute(cursor, inputs)
        self.logger.debug("Preparing to fetch a row.")
        result = cursor.fetchone()
        return result

    def fetch_many(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = None,
        size: Optional[int] = None,
        cursor_type: Type[SnowflakeCursor] = SnowflakeCursor,
        **execute_kwargs: Dict[str, Any],
    ) -> List[Tuple[Any]]:

        inputs = dict(
            command=operation,
            params=parameters,
            **execute_kwargs,
        )

        new, cursor = self._get_cursor(inputs, cursor_type=cursor_type)
        if new:
            self._execute(cursor, inputs)
        self.logger.debug("Preparing to fetch many row.")
        result = cursor.fetchmany(size=size)
        return result

    def fetch_all(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = None,
        cursor_type: Type[SnowflakeCursor] = SnowflakeCursor,
        **execute_kwargs: Dict[str, Any],
    ) -> List[Tuple[Any]]:

        inputs = dict(
            command=operation,
            params=parameters,
            **execute_kwargs,
        )

        new, cursor = self._get_cursor(inputs, cursor_type=cursor_type)
        if new:
            self._execute(cursor, inputs)
        self.logger.debug("Preparing to fetch many row.")
        result = cursor.fetchall()
        return result

    def execute(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = None,
        cursor_type: Type[SnowflakeCursor] = SnowflakeCursor,
        **execute_kwargs: Dict[str, Any],
    ) -> None:

        self._start_connection()

        inputs = dict(
            command=operation,
            params=parameters,
            **execute_kwargs,
        )
        with self._connection.cursor(cursor_type) as cursor:
            cursor.execute(**inputs)
        self.logger.info(f"Executed the operation, {operation!r}.")

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

    def make_lineage_event_from_table(self, table, add_to="inputs"):
        """Return a dictionary of the lineage event for the query"""

        marquez_event = {
            "eventType": "RUNNING",
            "eventTime": prefect.context.get_run_context().start_time,
            "job": {"namespace": "prefect", "name": self.flow_id},
            "run": {"runId": self.flow_run_id},
            "inputs": [],
            "outputs": [],
            "producer": "https://prefect.io",
        }

        schema_fields = self._get_table_schema(table)
        io_record = {
            "namespace": "prefect",
            "name": f"{self.default_namespace}/{table}",
            "facets": {
                "schema": {
                    "fields": schema_fields,
                    "_producer": "prefect",
                    "_schemaURL": "https://example.com",
                }
            },
        }
        marquez_event[add_to].append(io_record)

        return marquez_event

    def make_lineage_event_from_query(self, query):
        """Return a dictionary of the lineage event for the query"""

        tables_queried = self._list_tables_selected_in_query(query)
        tables_created = self._find_created_or_inserted_tables(query)

        full_queried_table_references = [
            self._get_full_tablereference(table) for table in tables_queried
        ]
        full_created_table_references = [
            self._get_full_tablereference(table) for table in tables_created
        ]

        marquez_event = {
            "eventType": "RUNNING",
            "eventTime": prefect.context.get_run_context().start_time,
            "job": {"namespace": "prefect", "name": self.flow_id},
            "run": {"runId": self.flow_run_id},
            "inputs": [],
            "outputs": [],
            "producer": "https://prefect.io",
        }

        for table in tables_created:
            schema_fields = self._get_table_schema(table)
            output_record = {
                "namespace": "prefect",
                "name": f"{self.default_namespace}/{table}",
                "facets": {
                    "schema": {
                        "fields": schema_fields,
                        "_producer": "prefect",
                        "_schemaURL": "https://example.com",
                    }
                },
            }
            marquez_event["outputs"].append(output_record)

        for table in tables_queried:
            schema_fields = self._get_table_schema(table)
            input_record = {
                "namespace": "prefect",
                "name": f"{self.default_namespace}/{table}",
                "facets": {
                    "schema": {
                        "fields": schema_fields,
                        "_producer": "prefect",
                        "_schemaURL": "https://example.com",
                    }
                },
            }
            marquez_event["inputs"].append(input_record)

        return marquez_event

    def _parse_full_table_name(
        self, full_table_name: str
    ) -> tuple[str | None, str | None, str]:
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

    def _get_full_tablereference(self, extracted_reference: str) -> str:
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

        engine = create_engine(self.connection_string)
        result = engine.execute(query)
        return [{"name": row["name"], "type": row["type"]} for row in result]

    def post_to_marquez(self, data):
        headers = {"Content-type": "application/json"}
        sendable_data = json.loads(json.dumps(data, ensure_ascii=True, default=str))
        r = requests.post(self.marquez_endpoint, json=sendable_data, headers=headers)

        if not r.ok:
            raise Exception(f"Error posting to OpenLineage: {r.status_code}, {r.text}")

    def complete_run(self):
        """Mark the run as complete"""
        marquez_event = {
            "eventType": "COMPLETE",
            "eventTime": datetime.datetime.now().isoformat(),
            "job": {"namespace": "prefect", "name": self.flow_id},
            "run": {"runId": self.flow_run_id},
            "inputs": [],
            "outputs": [],
            "producer": "https://prefect.io",
        }

        print(json.dumps(marquez_event, ensure_ascii=True, default=str))
        if self.marquez_endpoint:
            self.post_to_marquez(marquez_event)
