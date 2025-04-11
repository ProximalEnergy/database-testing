from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Get the data directory path
data_dir = Path("data_stats")

# Read all CSV files
dfs = {}
for csv_file in data_dir.glob("*.csv"):
    db_name = csv_file.stem  # Get filename without extension
    dfs[db_name] = pd.read_csv(csv_file)
    dfs[db_name] = dfs[db_name].sort_values(by="data_points")

# Create subplot with 2 rows
fig = make_subplots(
    rows=2,
    cols=1,
    subplot_titles=("Database Size vs Data Points", "Insert Time vs Data Points"),
    vertical_spacing=0.2,
)

# Add scatter points for each database
colors = {
    "clickhouse": "#F7A35C",
    "questdb": "#90ED7D",
    "timescale": "#7CB5EC",
    "tdengine": "#FF6B6B",
    "cratedb": "#FFD700",
    "influxdb": "#008000",
}

for db_name, df in dfs.items():
    # Table size plot (top)
    fig.add_trace(
        go.Scatter(
            name=db_name.capitalize(),
            x=df["data_points"],
            y=df["table_size_B"] / 1024 / 1024,
            mode="markers+lines",
            marker=dict(
                color=colors.get(db_name),
                size=10,
            ),
            legendgroup=db_name,
            showlegend=True,
        ),
        row=1,
        col=1,
    )

    # Insert time plot (bottom)
    fig.add_trace(
        go.Scatter(
            name=db_name.capitalize(),
            x=df["data_points"],
            y=df["insert_time_s"],
            mode="markers+lines",
            marker=dict(
                color=colors.get(db_name),
                size=10,
            ),
            legendgroup=db_name,
            showlegend=False,
        ),
        row=2,
        col=1,
    )

# Update layout
fig.update_layout(
    title="Database Performance Comparison",
    template="plotly_white",
    legend_title="Database",
    height=800,  # Make the plot taller to accommodate both subplots
)

# Update axes labels
fig.update_xaxes(title_text="Number of Data Points", row=1, col=1)
fig.update_xaxes(title_text="Number of Data Points", row=2, col=1)
fig.update_yaxes(title_text="Table Size (MiB)", row=1, col=1)
fig.update_yaxes(title_text="Insert Time (seconds)", row=2, col=1)

# Save the plot as HTML
fig.write_html("database_comparison.html")

print("Plot has been saved as database_comparison.html")
