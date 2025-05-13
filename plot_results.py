#!/usr/bin/env -S uv run --no-project

# /// script
# dependencies = ["plotly", "kaleido", "numpy", "pandas"]
# ///

import plotly.express as px

methods = [
    "psycopg2", "psycopg3", "asyncpg",
    "connectorx (pandas)", "connectorx (polars)"
]
times = [61.06, 67.66, 28.33, 14.31, 11.27]

fig = px.bar(
    x=times,
    y=methods,
    orientation='h',
    color=methods,
    color_discrete_sequence=px.colors.qualitative.Safe,
    labels={'x': 'Elapsed Time (seconds)', 'y': 'Library'},
    title="10M rows from QuestDB into Python via the PostgreSQL protocol",
    text=[f"{t:.2f}s" for t in times]
)

fig.update_layout(
    yaxis={'categoryorder': 'total ascending'},
    font=dict(size=14),
    showlegend=False
)

fig.write_image("benchmark_results.svg")
print("SVG chart saved as 'benchmark_results.svg'")