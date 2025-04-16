import os
import time

import pandas as pd
import requests
from dotenv import load_dotenv
from influxdb_client_3 import InfluxDBClient3

import data_generation
import utils
from config import config


def get_bucket_id(*, bucket_name: str) -> str | None:
    response = requests.get(
        "https://us-east-1-1.aws.cloud2.influxdata.com/api/v2/buckets",
        headers={
            "Authorization": f"Token {os.getenv('INFLUXDB_TOKEN')}",
            "Content-Type": "application/json",
        },
    )
    response.raise_for_status()

    data = response.json()

    for b in data["buckets"]:
        if b["name"] == bucket_name:
            return b["id"]

    return None


def create_bucket(*, bucket_name: str) -> None:
    response = requests.post(
        "https://us-east-1-1.aws.cloud2.influxdata.com/api/v2/buckets",
        headers={
            "Authorization": f"Token {os.getenv('INFLUXDB_TOKEN')}",
            "Content-Type": "application/json",
        },
        json={
            "orgID": "417ef56b0722c876",
            "name": bucket_name,
            "retentionRules": [{"type": "expire", "everySeconds": 0}],
        },
    )
    response.raise_for_status()

    return response.json()["id"]


def delete_bucket(*, bucket_id: str) -> None:
    response = requests.delete(
        f"https://us-east-1-1.aws.cloud2.influxdata.com/api/v2/buckets/{bucket_id}",
        headers={"Authorization": f"Token {os.getenv('INFLUXDB_TOKEN')}"},
    )
    response.raise_for_status()


def insert_dataframe(
    client, bucket: str, org: str, measurement: str, df: pd.DataFrame
) -> float:
    """
    Insert a pandas DataFrame into InfluxDB Cloud as points.
    """
    t_start = time.time()

    # Convert value_bool to int if necessary (InfluxDB fields are numeric)
    df["value_bool"] = df["value_bool"].astype("Int32")

    # Process DataFrame in chunks of 10000 rows
    chunk_size = 10000
    for i in range(0, len(df), chunk_size):
        chunk_df = df.iloc[i : i + chunk_size]
        client._write_api.write(
            bucket="data_timeseries",
            record=chunk_df,
            data_frame_measurement_name=measurement,
            data_frame_tag_columns=["tag_id"],
            data_frame_timestamp_column="time",
        )

    t_end = time.time()
    return round(t_end - t_start, 3)


def main():
    load_dotenv(override=True)

    token = os.getenv("INFLUXDB_TOKEN")
    org = "Project Data"
    host = "https://us-east-1-1.aws.cloud2.influxdata.com"
    client = InfluxDBClient3(
        host=host, token=token, org=org, database="data_timeseries"
    )

    bucket_name = "data_timeseries"
    bucket_id = get_bucket_id(bucket_name=bucket_name)
    if bucket_id:
        delete_bucket(bucket_id=bucket_id)
    create_bucket(bucket_name=bucket_name)

    data = []

    for minutes in config["minutes"]:
        for n_tags in config["tags"]:
            for seconds_interval in config["seconds_interval"]:
                try:
                    case_name = data_generation.generate_case_name(
                        minutes=minutes,
                        n_tags=n_tags,
                        seconds_interval=seconds_interval,
                    )

                    print(case_name)

                    table_name = case_name

                    df = pd.read_parquet(f"data/{case_name}.parquet")
                    df["time"] = pd.to_datetime(df["time"])
                    # Set month of all times to April (month 4)
                    df["time"] = df["time"].apply(lambda x: x.replace(month=4))

                    insert_time = insert_dataframe(
                        client=client,
                        bucket=table_name,
                        org=org,
                        measurement=table_name,
                        df=df,
                    )
                    print(f"\t{round(insert_time, 3)} s")
                    print(f"\t{int(len(df) / insert_time)} rows/s")

                    data.append(
                        {
                            "n_tags": n_tags,
                            "seconds_interval": seconds_interval,
                            "data_points": len(df),
                            # "table_size_B": table_size,
                            "insert_time_s": insert_time,
                            "remote": utils.get_remote(),
                        }
                    )
                except Exception as e:
                    print(f"Error: {e}")

    df_stats = pd.DataFrame(data)
    df_stats.to_csv("data_stats/influxdb.csv", index=False)


if __name__ == "__main__":
    main()


# def delete_measurement(*, bucket: str, org: str, measurement: str) -> None:
#     response = requests.post(
#         "https://us-east-1-1.aws.cloud2.influxdata.com/api/v2/delete",
#         headers={"Authorization": f"Token {os.getenv('INFLUXDB_TOKEN')}"},
#         params={
#             "bucket": bucket,
#             # "org": "417ef56b0722c876",
#         },
#         json={
#             "start": "2019-08-24T14:15:22Z",
#             "stop": "2025-08-24T14:15:22Z",
#             "predicate": '_measurement = "5_minutes_of_1000_tags_at_300_second_intervals"',
#         },
#     )
#     response.raise_for_status()
