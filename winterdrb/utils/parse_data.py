"""
Module for parsing avro data
"""

from pathlib import Path

import pandas as pd
from fastavro import reader

from winterdrb.filtering import apply_clean_filter
from winterdrb.paths import get_combined_avro_path, real_path, bogus_path
from winterdrb.utils.combine_avro import combine_avro_files


def load_avro(avro_path: Path) -> pd.DataFrame:
    """
    Load the avro file into a pandas DataFrame.

    :param avro_path: Avro file to load.
    :return: DataFrame of data.
    """

    records = []
    with open(avro_path, "rb") as avro_f:
        avro_reader = reader(avro_f)
        for record in avro_reader:
            records.append(record)

    unpacked = []

    for record in records:
        new = {}

        for field in [
            "objectid",
            "cutout_science",
            "cutout_template",
            "cutout_difference",
        ]:
            new[field] = record[field]

        # Prepend the latest observation to the history
        latest = pd.DataFrame(
            [{x: record["candidate"][x] for x in ["jd", "magpsf", "sigmapsf"]}]
        )
        hist = pd.concat(
            [pd.DataFrame(record["prv_candidates"]), latest], axis=0, ignore_index=True
        )
        new["prv_candidates"] = hist

        for field in record["candidate"]:
            new[field] = record["candidate"][field]

        unpacked.append(new)

    res = pd.DataFrame(unpacked)

    return res


def parse_night_data(night: str | int) -> pd.DataFrame:
    """
    Load the data for a given night into a pandas DataFrame.

    :param night: Night to load data for.
    :return: DataFrame of data.
    """

    avro_path = get_combined_avro_path(night)

    if not avro_path.exists():
        combine_avro_files(night)
    return load_avro(avro_path)


def get_cleaned_night_data(night: str | int) -> pd.DataFrame:
    """
    Load the data for a given night into a pandas DataFrame, and apply the clean filter.

    :param night: Night to load data for.
    :return: Cleaned DataFrame of data.
    """
    all_data = parse_night_data(night)
    cut = apply_clean_filter(all_data)
    return cut

def load_real_and_bogus() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load the real data from the real_path parqquet file.
    """
    real_df = pd.read_parquet(real_path)
    bogus_df = pd.read_parquet(bogus_path)
    return real_df, bogus_df
