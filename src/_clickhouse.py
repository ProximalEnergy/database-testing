import os
import time

import clickhouse_connect
import pandas as pd
from dotenv import load_dotenv

import data_generation  # type: ignore
import utils  # type: ignore
from config import config  # type: ignore


def create_table(client, table_name: str) -> None:
    client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            `time` DateTime64,
            `tag_id` UInt32,
            `value_int` Nullable(Int32),
            `value_float` Nullable(Float32),
            `value_str` Nullable(String),
            `value_bool` Nullable(UInt8)
        )
        ENGINE = SharedMergeTree
        PRIMARY KEY (tag_id, time);
        """
    )

    if "1_second_intervals" in table_name:
        client.command(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name}_1min (
                `time` DateTime64,
                `tag_id` UInt32,
                `value_int` Nullable(Int32),
                `value_float` Nullable(Float32),
                `value_str` Nullable(String),
                `value_bool` Nullable(UInt8)
            )
            ENGINE = SharedMergeTree
            PRIMARY KEY (tag_id, time);
            """
        )

        client.command(
            f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name}_mv TO {table_name}_1min AS
            SELECT
                toStartOfMinute(time) as time,
                tag_id,
                argMin(value_int, time) as value_int,
                argMin(value_float, time) as value_float,
                argMin(value_str, time) as value_str,
                argMin(value_bool, time) as value_bool
            FROM {table_name}
            GROUP BY time, tag_id
            ORDER BY time, tag_id;
            """
        )


def delete_table(client, table_name: str) -> None:
    """
    Drop a table if it exists.
    """
    client.command(f"DROP TABLE IF EXISTS {table_name}")
    client.command(f"DROP TABLE IF EXISTS {table_name}_1min")
    client.command(f"DROP TABLE IF EXISTS {table_name}_mv")


def get_table_size(client, table_name: str) -> int:
    """
    Get total size on disk for the table (in bytes).
    """
    # Query to get table size
    query = f"""
    SELECT
        total_bytes
    FROM system.tables
    WHERE database = 'default' AND name = '{table_name}'
    """

    # Execute the query
    result = client.query(query)

    # result is a list of tuples; we take the first row, first column.
    return result.result_rows[0][0]


def insert_dataframe(
    client, table_name: str, df: pd.DataFrame, chunksize: int
) -> float:
    """
    Insert a pandas DataFrame into ClickHouse.
    """
    t_start = time.time()

    # Convert value_bool to 0/1 for ClickHouse's UInt8
    df["value_bool"] = df["value_bool"].astype("Int32")

    df = df.replace({pd.NA: None})

    rows = df.values.tolist()

    # Chunk the insert into groups of 100,000 rows
    for i in range(0, len(rows), chunksize):
        chunk = rows[i : i + chunksize]
        client.insert(table_name, chunk, column_names=df.columns.tolist())

    t_end = time.time()
    return round(t_end - t_start, 3)


def main():
    load_dotenv(override=True)

    client = clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST"),
        port=8443,
        username="default",
        password=os.getenv("CLICKHOUSE_PASSWORD"),
    )

    data = []

    for minutes in config["minutes"]:
        for n_tags in config["tags"]:
            for seconds_interval in config["seconds_interval"]:
                case_name = data_generation.generate_case_name(
                    minutes=minutes,
                    n_tags=n_tags,
                    seconds_interval=seconds_interval,
                )

                print(case_name)

                table_name = f"_{case_name}"

                df = pd.read_parquet(f"data/{case_name}.parquet")

                # Start from scratch in each loop
                delete_table(client, table_name)
                create_table(client, table_name)

                insert_time = insert_dataframe(
                    client, table_name, df, chunksize=1_500_000
                )
                print(f"\t{round(insert_time, 3)} s")
                print(f"\t{int(len(df) / insert_time)} rows/s")
                table_size = get_table_size(client, table_name)

                data.append(
                    {
                        "n_tags": n_tags,
                        "seconds_interval": seconds_interval,
                        "data_points": len(df),
                        "table_size_B": table_size,
                        "insert_time_s": insert_time,
                        "remote": utils.get_remote(),
                    }
                )

                # If you want to drop after:
                # delete_table(client, table_name)

    df_stats = pd.DataFrame(data)
    df_stats.to_csv("data_stats/clickhouse.csv", index=False)


if __name__ == "__main__":
    main()
