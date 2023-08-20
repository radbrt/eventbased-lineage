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
from blocklineage import DataOperations
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
    _connection: Optional[SnowflakeConnection] = None
    _block_type_name = "SnowflakeLineage"
    _block_type_slug = "snowflake-lineage"


    @property
    def default_namespace(self):
        return f"snowflake://{self.account}/{self.database}/{self.db_schema}"


    def connect(self):
        return create_engine(self.connection_string).connect()


    @property
    def connection(self):
        """Return a connection to Snowflake."""
        if self._connection:
            return self._connection
        else:
            self._connection = self.connect()
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


    def read_sql(self, sql, **kwargs):

        if has_table(sql, self.connection):
            lineage_event = self.make_lineage_payload_from_table(sql, "input")
        else:
            lineage_event = self.make_lineage_payload_from_query(sql)

        return pd.read_sql(sql, con=self.connection, **kwargs)


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
        parsed = parse_one(sql_query)
        for e in parsed.find_all(exp.Select):
            for tbl in e.find_all(exp.Table):
                yield f"{tbl.catalog or self.db}.{tbl.db or self.schema}.{tbl.name}"


    def _list_tables_created_in_query(self, sql_query):
        parsed = parse_one(sql_query)
        for tbl in parsed.find_all(exp.Create):
            yield f"{tbl.this.catalog or self.this.db}.{tbl.db or self.schema}.{tbl.name}"

    def _list_tables_inserted_in_query(self, sql_query):
        parsed = parse_one(sql_query)
        for tbl in parsed.find_all(exp.Insert):
            yield f"{tbl.this.catalog or self.this.db}.{tbl.db or self.schema}.{tbl.name}"



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

        
        tbls = self._find_created_or_inserted_tables(operation)
        r = self.connection.execute(operation)

        for t in tbls:
            uri = f"{self.default_namespace}/{t}"
            self.emit_lineage_to_prefect(uri, "create", None)


        return r
    

    def make_lineage_payload_from_table(self, table, add_to="inputs"):
        """Return a dictionary of the lineage event for the query"""

        uri = f"{self.default_namespace}/{table}"

        openlineage_event = {
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
            "name": uri,
            "facets": {
                "schema": {
                    "fields": schema_fields,
                    "_producer": "prefect",
                    "_schemaURL": "https://example.com",
                }
            },
        }
        openlineage_event[add_to].append(io_record)

        return openlineage_event


    def make_lineage_payload_from_query(self, query):
        """Return a dictionary of the lineage event for the query"""

        tables_queried = self._list_tables_selected_in_query(query)
        tables_created = self._list_tables_created_in_query(query)
        tables_inserted = self._list_tables_inserted_in_query(query)


        openlineage_io = {
            "inputs": [],
            "outputs": []
        }

        for table in tables_created + tables_inserted:
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
            openlineage_io["outputs"].append(output_record)

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


    def make_event(self, data):
        sendable_data = json.loads(json.dumps(data, ensure_ascii=True, default=str))


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

        if self.marquez_endpoint:
            self.post_to_marquez(marquez_event)
