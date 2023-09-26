"""
Module for parsing avro data
"""
import pandas as pd
from fastavro import reader

from winterdrb.filtering import apply_clean_filter
from winterdrb.paths import get_combined_avro_path
from winterdrb.utils.combine_avro import combine_avro_files


def parse_night_data(night: str | int) -> pd.DataFrame:
    """
    Load the data for a given night into a pandas DataFrame.

    :param night: Night to load data for.
    :return: DataFrame of data.
    """

    avro_path = get_combined_avro_path(night)

    if not avro_path.exists():
        combine_avro_files(night)

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

    night_df = pd.DataFrame(unpacked)

    return night_df


def get_cleaned_night_data(night: str | int) -> pd.DataFrame:
    """
    Load the data for a given night into a pandas DataFrame, and apply the clean filter.

    :param night: Night to load data for.
    :return: Cleaned DataFrame of data.
    """
    all_data = parse_night_data(night)
    cut = apply_clean_filter(all_data)
    return cut
