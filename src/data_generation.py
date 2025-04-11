import string

import numpy as np
import pandas as pd

from config import config


def generate_case_name(*, minutes: int, n_tags: int, seconds_interval: int) -> str:
    return f"{minutes}_minutes_of_{n_tags}_tags_at_{seconds_interval}_second_intervals"


def generate_random_strings(*, n: int, length: int = 10) -> np.ndarray:
    characters = np.array(list(string.ascii_letters + string.digits))
    indices = np.random.randint(0, characters.size, size=(n, length))
    return np.array(["".join(row) for row in characters[indices]])


def generate_dataframe(
    *, minutes: int, n_tags: int, seconds_interval: int
) -> pd.DataFrame:
    total_seconds = minutes * 60

    # Calculate how many data points we need based on the interval
    num_points = total_seconds // seconds_interval

    # Create time points once
    time_points = [
        pd.Timestamp("2025-01-01 00:00:00")
        + pd.Timedelta(seconds=point * seconds_interval)
        for point in range(num_points)
    ]

    data: dict[str, list] = {
        "time": [],
        "tag_id": [],
        "value_int": [],
        "value_float": [],
        "value_str": [],
        "value_bool": [],
    }

    # Generate random data
    # 35% ints, 35% floats, 15% strings, 15% booleans
    for tag_id in range(n_tags):
        data["time"].extend(time_points)
        data["tag_id"].extend([tag_id] * num_points)

        if tag_id < n_tags * 0.35:
            data["value_int"].extend(
                np.random.randint(0, 101, size=num_points).tolist()  # type: ignore
            )
            data["value_float"].extend([None] * num_points)
            data["value_str"].extend([None] * num_points)
            data["value_bool"].extend([None] * num_points)
        elif tag_id < n_tags * 0.7:
            data["value_int"].extend([None] * num_points)
            data["value_float"].extend(np.random.rand(num_points).tolist())  # type: ignore
            data["value_str"].extend([None] * num_points)
            data["value_bool"].extend([None] * num_points)
        elif tag_id < n_tags * 0.85:
            data["value_int"].extend([None] * num_points)
            data["value_float"].extend([None] * num_points)
            data["value_str"].extend(generate_random_strings(n=num_points).tolist())
            data["value_bool"].extend([None] * num_points)
        else:
            data["value_int"].extend([None] * num_points)
            data["value_float"].extend([None] * num_points)
            data["value_str"].extend([None] * num_points)
            data["value_bool"].extend(
                (np.random.random(num_points) > 0.5).tolist()  # type: ignore
            )

    # Create DataFrame directly from the data dictionary
    df = pd.DataFrame(data)

    # Sort the data
    df = df.sort_values(by=["time", "tag_id"]).reset_index(drop=True)

    df = df.astype(
        {
            "value_int": "Int64",
            "value_float": "Float64",
            "value_str": "string",
            "value_bool": "boolean",
        }
    )
    return df


def main():
    for minutes in config["minutes"]:
        for n_tags in config["tags"]:
            for seconds_interval in config["seconds_interval"]:
                case_name = generate_case_name(
                    minutes=minutes, n_tags=n_tags, seconds_interval=seconds_interval
                )
                print(case_name)

                df = generate_dataframe(
                    minutes=minutes, n_tags=n_tags, seconds_interval=seconds_interval
                )
                df.to_parquet(f"data/{case_name}.parquet")


if __name__ == "__main__":
    main()
