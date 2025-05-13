import typer

from .load import load
from .query.asyncpg import benchmark as bench_asyncpg

app = typer.Typer(help=__doc__, add_completion=False)

app.command()(load)
app.command(name="bench_asyncpg")(bench_asyncpg)


if __name__ == "__main__":
    app(prog_name="qdbc")
