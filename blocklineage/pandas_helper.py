import json

import pandas as pd
import requests
from pandas.io.sql import has_table
from sqlalchemy import create_engine


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

        print(json.dumps(lineage_event, ensure_ascii=True, default=str))
        if block.marquez_endpoint:
            block.post_to_marquez(lineage_event)

        return pd.read_sql(sql, **kwargs)

    def to_sql(self, sql, **kwargs):
        block = kwargs.pop("con")
        con = block.connection
        kwargs["con"] = con

        lineage_event = block.make_lineage_event_from_table(sql, "output")

        print(json.dumps(lineage_event, ensure_ascii=True, default=str))
        if block.marquez_endpoint:
            block.post_to_marquez(lineage_event)

        return self.df.to_sql(sql, **kwargs)
