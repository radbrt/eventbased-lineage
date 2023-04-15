import prefect
from prefect import task, flow, get_run_logger
from ..blocklineage import SnowflakeLineageBlock
import pandas as pd


@task
def get_some_data_from_table(lineageblock):
    query = "select * from nkoder"
    df = pd.read_csv(query, con=lineageblock)

    return df


@task
def get_n2_count(df):

    df["n2"] = df["naerk"].astype(str).apply(lambda x: x[0:2])

    return df.groupby('n2').agg(n_subgroups=('n2', 'size')).reset_index()


@flow
def main_flow():
    sf = SnowflakeLineageBlock.load('ax')
    df = get_some_data_from_table(lineageblock=sf)
    df = get_n2_count(df)
    df.lb.to_sql("n2koder", con=sf, if_exists="replace", index=False)

    sf.complete_run()


if __name__ == "__main__":
    main_flow()