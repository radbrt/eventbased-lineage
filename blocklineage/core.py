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
from prefect.events import emit_event


class LineageBlock(Block):
    """Generic class for Lineage blocks containing common functionality"""

    backend: Optional[str] = "marquez"
    marquez_endpoint: Optional[str] = None
    marquez_api_token: Optional[SecretStr] = None
    marquez_namespace: Optional[str] = None

    _block_type_name = "GenericLineage"
    _block_type_slug = "generic-lineage"


    def __init__(self,
            backend,
            marquez_endpoint,
            marquez_api_token,
            marquez_namespace
        ):

        super.__init__()
        self.backend = backend
        self.marquez_endpoint = marquez_endpoint
        self.marquez_api_token = marquez_api_token
        self.marquez_namespace = marquez_namespace


    @property
    def flow_run_id(self):
        return prefect.runtime.flow_run.id


    @property
    def flow_run_name(self):
        return prefect.runtime.flow_run.name


    @property
    def flow_name(self):
        return "prefect.flow-run." + prefect.runtime.flow_run.flow_name


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


    def _emit_lineage_to_prefect(self, uri: str, operation: str, schema_fields):

        io_record = {
            "namespace": "prefect",
            "name": uri
        }

        if schema_fields:
            io_record["facets"] = {
                    "schema": {
                        "fields": schema_fields,
                        "_producer": "prefect",
                        "_schemaURL": "https://example.com",
                    }
            }

        emitted = emit_event(
            event=f"lineage.{operation}",
            occurred=datetime.datetime.utcnow(),
            resource={
                "lineage.resource.uri": uri
            },
            payload=io_record
        )

        return emitted


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


    def make_lineage_event_from_table(self, table, add_to="inputs"):
        """Return a dictionary of the lineage event for the query"""

        uri = f"{self.default_namespace}/{table}"

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
            "name": uri,
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
        """Must be implemented in subclass: return the schema for the table"""
        pass


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

        # print(json.dumps(marquez_event, ensure_ascii=True, default=str))
        if self.marquez_endpoint:
            self.post_to_marquez(marquez_event)
