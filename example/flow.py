import prefect
from prefect import task, flow, get_run_logger
from blocklineage import SnowflakeLineageBlock
import pandas as pd
from prefect.events import emit_event
import datetime 
@task
def get_some_data_from_table(lineageblock):
    query = "select * from nkoder"
    df = pd.read_csv(query, con=lineageblock)

    return df


# @task
# def get_n2_count(df):

#     df["n2"] = df["naerk"].astype(str).apply(lambda x: x[0:2])

#     return df.groupby('n2').agg(n_subgroups=('n2', 'size')).reset_index()


@flow
def main_flow():

    emit_event(
        event="lineage_canary", 
        resource={
            "lineage.canary": "canary",
            "prefect.resource.id": "abc123"
            }, 
        occurred=datetime.datetime.now() 
    )
    sf = SnowflakeLineageBlock.load('ax')

    r = sf.execute("SELECT 1")
    rr = r.fetchall()

    l = get_run_logger()

    l.info(rr)
    # df = get_some_data_from_table(lineageblock=sf)
    # df = get_n2_count(df)
    # df.lb.to_sql("n2koder", con=sf, if_exists="replace", index=False)

    # sf.complete_run()


if __name__ == "__main__":
    main_flow()