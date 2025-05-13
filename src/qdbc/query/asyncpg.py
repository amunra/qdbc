import asyncpg
import time

from .utils import syncify

@syncify
async def benchmark(
    host: str = 'localhost',
    pgport: int = 8812,
    user: str = 'admin',
    password: str = 'quest'
):
    """
    Benchmark the asyncpg library by fetching data from QuestDB.
    """
    conn = await asyncpg.connect(
        host=host,
        port=pgport,
        user=user,
        password=password,
        database='qdb',
    )
    

    s = time.time_ns()
 
    rows = await conn.fetch("SELECT * FROM cpu")
    obj = None
    for row in rows:
        for cell in row.values():
            obj = cell

    e = time.time_ns()

    # print(f"Object: {obj}")

    elapsed = (e - s) / 1_000_000_000
    print(f"Elapsed time: {elapsed:.2f} seconds processing {len(rows)} rows")

    await conn.close()
