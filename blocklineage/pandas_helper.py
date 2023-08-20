import json

import pandas as pd
import requests
from pandas.io.sql import has_table
from sqlalchemy import create_engine
from blocklineage.utils import DataOperations


@pd.api.extensions.register_dataframe_accessor("lb")
class PandasLineageHelper:
    def __init__(self, pandas_obj):
        self.df = pandas_obj

    def read_sql(self, sql, **kwargs):
        block = kwargs.pop("con")
        con = block.connection
        kwargs["con"] = con

        if has_table(sql, con):
            lineage_event = block.make_lineage_event_from_table(sql, "input")
        else:
            lineage_event = block.make_lineage_event_from_sql(sql)

        if block.marquez_endpoint:
            block.post_to_marquez(lineage_event)

        return pd.read_sql(sql, **kwargs)

    def get_schema(self):
        schema = []
        for col in self.df.columns:
            schema.append({"name": col, "type": str(self.df[col].dtype)})

        return schema
        

    def to_sql(self, sql, **kwargs):
        """Write records stored in a DataFrame to a SQL database."""

        block = kwargs.pop("con")
        con = block.connection
        full_table_ref = block.get_full_tablereference(sql)

        operation_type = kwargs.get("if_exists", "fail")
        if operation_type == "replace":
            event_operation = DataOperations.CREATE
        elif operation_type == "append":
            event_operation = DataOperations.WRITE
        else:
            event_operation = DataOperations.CREATE

        # kwargs["con"] = con
        uri = f"{block.default_namespace}/{full_table_ref}"
        block.emit_lineage_to_prefect(uri, event_operation, self.get_schema())
        

        return self.df.to_sql(sql, con=con, **kwargs, index=False)
