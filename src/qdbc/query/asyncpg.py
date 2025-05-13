import asyncpg
import time


def syncify(func):
    """
    Decorator to convert an asynchronous function to a synchronous one.
    """
    import asyncio
    import functools

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return asyncio.run(func(*args, **kwargs))
    
    return wrapper


@syncify
async def benchmark(
    host: str = 'localhost',
    pgport: int = 8812,
    user: str = 'admin',
    password: str = 'quest'
):
    conn = await asyncpg.connect(
        host=host,
        port=pgport,
        user=user,
        password=password,
        database='qdb',
    )
    

    s = time.time_ns()
 
    rows = await conn.fetch("SELECT * FROM cpu")
    usage_user = 0
    for row in rows:
        usage_user += row['usage_user']

    e = time.time_ns()

    elapsed = (e - s) / 1_000_000_000
    print(f"Elapsed time: {elapsed:.2f} seconds processing {len(rows)} rows")

    await conn.close()
