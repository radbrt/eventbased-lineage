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
from blocklineage.utils import DataOperations


class LineageBlock(Block):
    """Generic class for Lineage blocks containing common functionality"""


    _block_type_name = "GenericLineage"
    _block_type_slug = "generic-lineage"


    @property
    def default_schema_uri(self):
        return "http://localhost"


    @property
    def prefect_resource_id(self):
        return "lineage"

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



    def emit_lineage_to_prefect(self, uri: str, operation: DataOperations, schema_fields: Optional[Dict], schema_url: Optional[str] = None):

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
            event=f"lineage.{operation.value}",
            occurred=datetime.datetime.utcnow(),
            resource={
                "lineage.resource.uri": uri,
                "prefect.resource.id": self.prefect_resource_id
            },
            payload=io_record
        )

        return None

