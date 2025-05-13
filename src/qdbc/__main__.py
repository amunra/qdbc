import typer

from .load import load
from .query.psycopg2 import benchmark as bench_psycopg2
from .query.psycopg3 import benchmark as bench_psycopg3
from .query.asyncpg import benchmark as bench_asyncpg
from .query.connectorx_polars import benchmark as bench_cx_polars
from .query.connectorx_pandas import benchmark as bench_cx_pandas

app = typer.Typer(help=__doc__, add_completion=False)

app.command()(load)
app.command(name="bench_psycopg2")(bench_psycopg2)
app.command(name="bench_psycopg3")(bench_psycopg3)
app.command(name="bench_asyncpg")(bench_asyncpg)
app.command(name="bench_cx_polars")(bench_cx_polars)
app.command(name="bench_cx_pandas")(bench_cx_pandas)


if __name__ == "__main__":
    app(prog_name="qdbc")
