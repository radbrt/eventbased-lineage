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
    def default_schema_uri(self):
        return "http://localhost"

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



    def emit_lineage_to_prefect(self, uri: str, operation: str, schema_fields: Optional[Dict], schema_url: Optional[str] = None):

        io_record = {
            "namespace": "prefect",
            "name": uri
        }

        if schema_fields:
            io_record["facets"] = {
                    "schema": {
                        "fields": schema_fields,
                        "_producer": "prefect",
                        "_schemaURL": schema_url or self.default_schema_uri,
                    }
            }

        emit_event(
            event=f"lineage.{operation}",
            occurred=datetime.datetime.utcnow(),
            resource={
                "lineage.resource.uri": uri,
                "prefect.resource.id": "abd12345"
            },
            payload=io_record
        )

        return None



    def mark_completed_run(self):
        """Mark the run as complete"""
        marquez_event = {
            "eventType": "COMPLETE",
            "eventTime": datetime.datetime.now().isoformat(),
            "job": {"namespace": "prefect", "name": self.flow_name},
            "run": {"runId": self.flow_run_id},
            "inputs": [],
            "outputs": [],
            "producer": "https://prefect.io",
        }

        # print(json.dumps(marquez_event, ensure_ascii=True, default=str))
        if self.marquez_endpoint:
            self.post_to_marquez(marquez_event)
