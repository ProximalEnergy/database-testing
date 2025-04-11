import os
import time

import pandas as pd
import requests
from dotenv import load_dotenv
from influxdb_client_3 import InfluxDBClient3, Point, WritePrecision

import data_generation
from config import config

load_dotenv()


def create_bucket(*, client, bucket: str, org: str) -> None:
    response = requests.post(
        "https://us-east-1-1.aws.cloud2.influxdata.com/api/v2/buckets",
        headers={
            "Authorization": f"Token {os.getenv('INFLUXDB_TOKEN')}",
            "Content-Type": "application/json",
        },
        json={"name": bucket, "orgID": "417ef56b0722c876"},
    )
    response.raise_for_status()

    return response.json()["id"]


def delete_bucket(*, client, bucket: str, org: str, bucket_id: str) -> None:
    response = requests.delete(
        f"https://us-east-1-1.aws.cloud2.influxdata.com/api/v2/buckets/{bucket_id}",
        headers={"Authorization": f"Token {os.getenv('INFLUXDB_TOKEN')}"},
    )
    response.raise_for_status()


def delete_measurement(*, client, bucket: str, org: str) -> None:
    response = requests.post(
        "https://us-east-1-1.aws.cloud2.influxdata.com/api/v2/delete",
        headers={"Authorization": f"Token {os.getenv('INFLUXDB_TOKEN')}"},
        json={
            "bucket": "data_timeseries",
        },
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

        points = []
        for _, row in chunk_df.iterrows():
            # Create a point for the measurement, using the "time" column as the timestamp
            point = Point(measurement).time(row["time"], WritePrecision.NS)
            # Add remaining columns as fields
            point.tag("tag_id", row["tag_id"])
            for col in chunk_df.columns:
                if col in ["time", "tag_id"]:
                    continue
                if pd.notna(row[col]):
                    point.field(col, row[col])
            points.append(point)

        client.write(record=points)

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

    # delete_measurement(
    #     client=client,
    #     bucket="data_timeseries",
    #     org=org,
    #     # measurement=table_name,
    # )

    data = []

    for minutes in config["minutes"]:
        for n_tags in config["tags"]:
            for seconds_interval in config["seconds_interval"]:
                case_name = data_generation.generate_case_name(
                    minutes=minutes, n_tags=n_tags, seconds_interval=seconds_interval
                )

                print(case_name)

                table_name = case_name

                # bucket_id = create_bucket(client=client, bucket=table_name, org=org)
                # delete_bucket(
                #     client=client, bucket=table_name, org=org, bucket_id=bucket_id
                # )
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

                data.append(
                    {
                        "n_tags": n_tags,
                        "seconds_interval": seconds_interval,
                        "data_points": len(df),
                        # "table_size_B": table_size,
                        "insert_time_s": insert_time,
                    }
                )


if __name__ == "__main__":
    main()
