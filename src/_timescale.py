import concurrent.futures
import itertools
import os
import time
from io import StringIO
from secrets import token_hex

import pandas as pd
import psycopg2
from dotenv import load_dotenv

import data_generation  # type: ignore
import utils  # type: ignore
from config import config  # type: ignore


def create_table(*, cursor: psycopg2.extensions.cursor, table_name: str) -> None:
    cursor.execute(
        f"""CREATE TABLE IF NOT EXISTS {table_name} (
            time TIMESTAMPTZ,
            tag_id INT,
            value_int INT,
            value_float FLOAT,
            value_str TEXT,
            value_bool BOOLEAN,
            PRIMARY KEY (time, tag_id)
        );
        """
    )
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS {table_name}_tag_id_time_idx ON {table_name} (tag_id, time DESC);"
    )
    cursor.execute(
        f"SELECT create_hypertable('{table_name}', 'time', chunk_time_interval => INTERVAL '5 minutes')"
    )


def delete_table(*, cursor: psycopg2.extensions.cursor, table_name: str) -> None:
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")


def get_table_size(*, cursor: psycopg2.extensions.cursor, table_name: str) -> int:
    # Get total table size in bytes
    cursor.execute(f"SELECT hypertable_size('{table_name}');")
    return cursor.fetchone()[0]  # type: ignore


def insert_dataframe(
    *,
    cursor: psycopg2.extensions.cursor,
    table_name: str,
    df: pd.DataFrame,
    chunksize: int,
    workers: int,
) -> float:
    # Update index to be current data (as of the latest 5 minutes)
    now = pd.Timestamp.utcnow().floor("5min").tz_localize(None)
    delta = now - pd.to_datetime(df["time"].min())
    df["time"] = df["time"] + delta

    t_start = time.time()

    chunks = []
    for i in range(0, len(df), chunksize):
        chunks.append(df.iloc[i : i + chunksize])

    def insert_chunk(chunk):
        random_id = token_hex(16)
        table_name_temp = f"{table_name}_temp_{random_id}"

        cursor.execute(f"DROP TABLE IF EXISTS {table_name_temp}")

        cursor.execute(
            f"""
            CREATE TEMPORARY TABLE {table_name_temp} AS
            SELECT * FROM {table_name}
            WITH NO DATA;
            """
        )

        sio = StringIO()
        sio.write(df.to_csv(sep="\t", index=False, header=False))
        sio.seek(0)

        cursor.copy_from(
            sio,
            table_name_temp,
            sep="\t",
            null="",
            columns=df.columns,
        )

        cursor.execute(
            f"""
            INSERT INTO {table_name}
            SELECT * FROM {table_name_temp}
            ON CONFLICT (time, tag_id) DO NOTHING;
            """
        )

        cursor.execute(f"DROP TABLE IF EXISTS {table_name_temp}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        executor.map(insert_chunk, chunks)

    t_end = time.time()
    return round(t_end - t_start, 3)


def main():
    load_dotenv(override=True)

    data = []

    cases = list(
        itertools.product(
            config["minutes"],
            config["workers"],
            config["tags"],
            config["seconds_interval"],
        )
    )

    for minutes, workers, n_tags, seconds_interval in cases:
        case_name = data_generation.generate_case_name(
            minutes=minutes, n_tags=n_tags, seconds_interval=seconds_interval
        )

        print(case_name)

        table_name = f"_{case_name}"

        df = pd.read_parquet(f"data/{case_name}.parquet")

        with psycopg2.connect(os.getenv("TIMESCALE_CONNECTION_STRING")) as conn:
            with conn.cursor() as cursor:
                # delete_table(cursor=cursor, table_name=table_name)
                # create_table(cursor=cursor, table_name=table_name)
                insert_time = insert_dataframe(
                    cursor=cursor,
                    table_name=table_name,
                    df=df,
                    chunksize=100_000,
                    workers=workers,
                )
                print(f"\t{round(insert_time, 3)} s")
                print(f"\t{int(len(df) / insert_time)} rows/s")

                table_size = get_table_size(cursor=cursor, table_name=table_name)

                data.append(
                    {
                        "n_tags": n_tags,
                        "seconds_interval": seconds_interval,
                        "data_points": len(df),
                        "table_size_B": table_size,
                        "insert_time_s": insert_time,
                    }
                )

                # delete_table(cursor=cursor, table_name=table_name)

    df_stats = pd.DataFrame(data)
    file_name = f"data_stats/timescale_{workers}_workers_{'remote' if utils.get_remote() else 'local'}.csv"
    df_stats.to_csv(file_name, index=False)


if __name__ == "__main__":
    main()
