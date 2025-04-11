import os

import taosrest  # type: ignore
from dotenv import load_dotenv

import data_generation
from config import config

load_dotenv()


def get_conn():
    url = os.getenv("TDENGINE_CLOUD_URL")
    token = os.getenv("TDENGINE_CLOUD_TOKEN")
    conn = taosrest.connect(url=url, token=token)
    return conn


def create_table(*, conn, table_name: str) -> None:
    conn.execute(
        f"""
        CREATE STable project_data.{table_name} (
            time timestamp,
            value_int int,
            value_float float,
            value_str binary(64),
            value_bool bool
        ) TAGS (
            tag_id int
        );"""
    )


def delete_table(*, conn, table_name: str) -> None:
    conn.execute(f"DROP STABLE IF EXISTS project_data.{table_name}")


def main():
    load_dotenv(override=True)

    # data = []

    for minutes in config["minutes"]:
        for n_tags in config["tags"]:
            for seconds_interval in config["seconds_interval"]:
                case_name = data_generation.generate_case_name(
                    minutes=minutes, n_tags=n_tags, seconds_interval=seconds_interval
                )

                print(case_name)

                table_name = f"_{case_name}"

                conn = get_conn()
                delete_table(conn=conn, table_name=table_name)
                create_table(conn=conn, table_name=table_name)


if __name__ == "__main__":
    main()
