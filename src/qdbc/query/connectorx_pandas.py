import pandas as pd

import time
import connectorx as cx


def benchmark(
    host: str = "localhost",
    pgport: int = 8812,
    user: str = "admin",
    password: str = "quest",
):
    """
    Benchmark the connectorx to pandas library by fetching data from QuestDB.
    """
    s = time.time_ns()
    df = cx.read_sql(
        conn=f'redshift://{user}:{password}@{host}:{pgport}/qdb',
        query='SELECT * FROM cpu',
        return_type="pandas",
    )
    e = time.time_ns()
    elapsed = (e - s) / 1_000_000_000
    print(f"Elapsed time: {elapsed:.2f} seconds processing {len(df)} rows")
    # print(f"Received DataFrame:")
    # print(df)
