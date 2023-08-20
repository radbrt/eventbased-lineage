import prefect
from prefect import task, flow, get_run_logger
from blocklineage import SnowflakeLineageBlock
import pandas as pd
from prefect.events import emit_event
import datetime 
import random
import blocklineage


def get_some_data(sf):
    """Read some data from a table"""

    df = pd.read_sql("select * from nkoder", con=sf.connection)
    return df


@task
def get_n2_count(df):
    """Count the number of rows per n2"""
    df["n2"] = df["naerk"].astype(str).apply(lambda x: x[0:2])

    return df.groupby('n2').agg(n_subgroups=('n2', 'size')).reset_index()


@flow
def main_flow():
    """Example docstring"""

    sf = SnowflakeLineageBlock.load('sfax')

    r = sf.execute("INSERT INTO landing.f2 VALUES (3, 'abc')")
    rr = r.fetchall()

    l = get_run_logger()

    l.info(rr)

    df = get_some_data(sf)
    df = get_n2_count(df)
    df.lb.to_sql("n2koder", con=sf, if_exists="replace", index=False)
    # df = get_some_data_from_table(lineageblock=sf)
    # df = get_n2_count(df)
    # df.lb.to_sql("n2koder", con=sf, if_exists="replace", index=False)

    # sf.complete_run()


if __name__ == "__main__":
    main_flow()