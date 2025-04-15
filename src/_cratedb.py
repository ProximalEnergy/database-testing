# https://cratedb.com/docs/guide/home/
# https://cratedb.com/hubfs/Gated-Content/Guide-for-Time-Series-Data-Projects.pdf
# https://cratedb.com/hubfs/Gated-Content/Guide-for-Time-Series-Data-Projects-Part-2.pdf
# https://community.cratedb.com/t/resampling-time-series-data-with-date-bin/1009

# INSERT INTO
#   <table_name> (
#     time,
#     tag_id,
#     value_int,
#     value_float,
#     value_str,
#     value_bool
#   )
# SELECT
#   time,
#   tag_id,
#   value_int,
#   value_float,
#   value_str,
#   value_bool
# FROM
#   (
#     SELECT
#       DATE_BIN('1 minute'::INTERVAL, "time", 0) AS time,
#       tag_id,
#       value_int,
#       value_float,
#       value_str,
#       value_bool,
#       ROW_NUMBER() OVER (
#         PARTITION BY
#           DATE_BIN('1 minute'::INTERVAL, "time", 0),
#           tag_id
#         ORDER BY
#           "time" ASC
#       ) AS row_number
#     FROM
#       doc.<table_name_raw>
#   ) x
# WHERE
#   row_number = 1;

# Error
# 5000
# CircuitBreakingException[Allocating 1mb for 'distWindowAgg: 1' failed, breaker would use 1gb in total. Limit is 1gb. Either increase memory and limit, change the query or reduce concurrent query load]

import os
import time

import pandas as pd
import sqlalchemy as sa
from crate import client  # type: ignore
from dotenv import load_dotenv
from sqlalchemy_cratedb.support import insert_bulk  # type: ignore

import data_generation
import utils
from config import config

load_dotenv(override=True)


def get_conn():
    return client.connect(
        os.getenv("CRATEDB_HOST"),
        username="admin",
        password=os.getenv("CRATEDB_PASSWORD"),
        verify_ssl_cert=True,
    )


def create_table(*, conn, table_name: str) -> None:
    # See more about partitions at https://cratedb.com/docs/crate/reference/en/latest/general/ddl/partitioned-tables.html#partitioned-tables
    # See more about shards at https://cratedb.com/docs/crate/reference/en/latest/general/ddl/sharding.html#ddl-sharding
    # See more about replicas at https://cratedb.com/docs/crate/reference/en/latest/general/ddl/replication.html

    # NOTE: It's recommended to have at least one replica for each shard, but two replicas would provide better fault tolerance.
    # NOTE: Shard size, which should be between approximately 3 and 70 GB.
    # NOTE: Each node should not have more than 1,000 shards.

    # TODO: Test configurable storage engine

    with conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS {table_name} (
                time TIMESTAMP NOT NULL,
                tag_id INT NOT NULL,
                value_int INT,
                value_float FLOAT,
                value_str TEXT,
                value_bool BOOLEAN,
                partition TIMESTAMP GENERATED ALWAYS AS DATE_TRUNC('day', "time"),
                PRIMARY KEY (time, tag_id, partition)
            )
            CLUSTERED INTO 1 SHARDS
            PARTITIONED BY (partition)
            WITH (
                "number_of_replicas" = 0
                -- "routing.allocation.require.storage" = 'cold'
            );
            """
        )


def delete_table(*, conn, table_name: str) -> None:
    with conn:
        cursor = conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")


def get_table_size(*, conn, table_name: str) -> int:
    # Get table size in bytes
    with conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT
                sum(size) as size
            FROM
                sys.shards
            WHERE
                table_name = '{table_name}'
            """
        )
        return cursor.fetchone()[0]  # type: ignore


def insert_dataframe(*, table_name: str, df: pd.DataFrame, chunksize: int) -> float:
    dburi = os.getenv("CRATEDB_CONNECTION_STRING")
    if dburi is None:
        raise ValueError("CRATEDB_CONNECTION_STRING is not set")

    t_start = time.time()

    engine = sa.create_engine(
        dburi,
        echo=False,  # Change to True to see detailed logging
    )

    for i in range(0, len(df), chunksize):
        df_chunk = df.iloc[i : i + chunksize]

        df_chunk.to_sql(
            name=table_name,
            con=engine,
            if_exists="append",
            index=False,  # Prevent SQLAlchemy from sending CREATE INDEX statements that aren’t needed with CrateDB
            chunksize=chunksize,
            method=insert_bulk,
        )

    t_end = time.time()
    return t_end - t_start


def main():
    load_dotenv(override=True)

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

                delete_table(conn=get_conn(), table_name=table_name)
                create_table(conn=get_conn(), table_name=table_name)

                df = pd.read_parquet(f"data/{case_name}.parquet")

                insert_time = insert_dataframe(
                    table_name=table_name, df=df, chunksize=250_000
                )
                print(f"\t{round(insert_time, 3)} s")
                print(f"\t{int(len(df) / insert_time)} rows/s")
                table_size = get_table_size(conn=get_conn(), table_name=table_name)

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

    df_stats = pd.DataFrame(data)
    df_stats.to_csv("data_stats/cratedb.csv", index=False)


if __name__ == "__main__":
    main()
