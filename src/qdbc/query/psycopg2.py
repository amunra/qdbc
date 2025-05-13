import time
import psycopg2


def benchmark(
    host: str = "localhost",
    pgport: int = 8812,
    user: str = "admin",
    password: str = "quest",
):
    """
    Benchmark the psycopg2 library by fetching data from QuestDB.
    """
    conn = psycopg2.connect(
        host=host,
        port=pgport,
        user=user,
        password=password,
        dbname='qdb',
    )

    s = time.time_ns()
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM cpu")
        rows = cur.fetchall()
        obj = None
        for row in rows:
            for cell in row:
                obj = cell

    e = time.time_ns()

    # print(f"Object: {obj}")

    elapsed = (e - s) / 1_000_000_000
    print(f"Elapsed time: {elapsed:.2f} seconds processing {len(rows)} rows")

    conn.close()