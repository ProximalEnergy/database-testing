import os
import time

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from questdb.ingress import Sender  # type: ignore

import data_generation  # type: ignore
from config import config  # type: ignore


def create_table(*, cursor: psycopg2.extensions.cursor, table_name: str) -> None:
    # === tag_id SYMBOL ===
    # See more at https://questdb.com/docs/concept/indexes/

    # === value_bool INT NULL ===
    # Nullable BOOLEANs are not supported.
    # See more at https://questdb.com/docs/reference/sql/datatypes/

    # === PARTITION BY DAY ===
    # Partition data by day. Could also be HOUR, WEEK, MONTH.

    # === DEDUP UPSERT KEYS(time, tag_id) ===
    # In order to ensure there are not duplicate (time, tag_id) values, we use the WAL and DEDUP options.
    # This will potentially have a negative impact on the write performance.
    # See more at https://questdb.com/docs/concept/deduplication/
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            time TIMESTAMP,
            tag_id SYMBOL,
            value_int INT NULL,
            value_float DOUBLE NULL,
            value_str STRING NULL,
            value_bool INT NULL
        ) TIMESTAMP(time) PARTITION BY DAY WAL
        DEDUP UPSERT KEYS(time, tag_id);
        """
    )


def delete_table(*, cursor: psycopg2.extensions.cursor, table_name: str) -> None:
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")


def get_table_size(*, cursor: psycopg2.extensions.cursor, table_name: str) -> int:
    cursor.execute(
        f"select diskSize from table_storage() where tableName = '{table_name}';"
    )
    res = cursor.fetchone()  # type: ignore
    return res[0]  # type: ignore


def insert_dataframe(*, table_name: str, df: pd.DataFrame) -> float:
    t_start = time.time()
    # Convert value_bool to Int32 before inserting
    df["value_bool"] = df["value_bool"].astype("Int32")
    conf = "http::addr=localhost:9000;"
    with Sender.from_conf(conf) as sender:
        sender.dataframe(df, table_name=table_name, at="time")
    t_end = time.time()
    return round(t_end - t_start, 3)


def main():
    load_dotenv(override=True)

    data = []

    for minutes in config["minutes"]:
        for n_tags in config["tags"]:
            for seconds_interval in config["seconds_interval"]:
                case_name = data_generation.generate_case_name(
                    minutes=minutes, n_tags=n_tags, seconds_interval=seconds_interval
                )
                print(case_name)

                table_name = f"_{case_name}"

                df = pd.read_parquet(f"data/{case_name}.parquet")

                # Connect to QuestDB via PostgreSQL wire protocol
                with psycopg2.connect(os.getenv("QUEST_CONNECTION_STRING")) as conn:
                    with conn.cursor() as cursor:
                        delete_table(cursor=cursor, table_name=table_name)
                        create_table(cursor=cursor, table_name=table_name)
                        insert_time = insert_dataframe(table_name=table_name, df=df)
                        table_size = get_table_size(
                            cursor=cursor, table_name=table_name
                        )
                        data.append(
                            {
                                "n_tags": n_tags,
                                "seconds_interval": seconds_interval,
                                "data_points": len(df),
                                "table_size_B": table_size,
                                "insert_time_s": insert_time,
                            }
                        )

    df_stats = pd.DataFrame(data)
    df_stats.to_csv("data_stats/questdb.csv", index=False)


if __name__ == "__main__":
    main()
