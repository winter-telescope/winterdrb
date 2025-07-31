"""
Module to combine avro files for a given night.
"""
import logging

import fastavro
from fastavro import reader

from winterdrb.paths import get_combined_avro_path, get_raw_avro_dir, real_path, bogus_path, AVRO_DIR
import pandas as pd
from winterdrb.utils.google import get_class_from_google

logger = logging.getLogger(__name__)


def flatten_records(records) -> pd.DataFrame:
    """
    Flatten the records from the avro files into a pandas DataFrame.

    :param records: List of records from the avro files.
    :return: DataFrame with flattened records.
    """
    df = pd.DataFrame(records)

    cand_df = pd.DataFrame(df["candidate"].tolist())
    df.drop(columns=["candidate", "prv_candidates"], inplace=True)

    df = pd.concat([df, cand_df], axis=1)
    df = df.where(pd.notnull(df), None)
    df = df.loc[:, ~df.columns.duplicated()]
    return df


def combine_avro_files(night: str | int):
    """
    Function to combine avro files for a given night.

    :param night: Night to combine avro files for.
    :return: None
    """
    avro_dir = get_raw_avro_dir(night)

    avro_files = list(avro_dir.glob("*.avro"))
    logging.info(f"Found {len(avro_files)} avro files")

    if len(avro_files) == 0:
        err = f"No avro files found for {night} at {avro_dir}"
        logging.error(err)
        raise FileNotFoundError(err)

    records = []
    for avro_path in avro_files:
        with open(avro_path, "rb") as avro_f:
            avro_reader = reader(avro_f)
            schema = avro_reader.writer_schema
            for record in avro_reader:
                records.append(record)

    logging.info(f"Found {len(records)} individual entries")

    out_path = get_combined_avro_path(night)

    logging.info(f"Saving to {out_path}")

    with open(out_path, "wb") as out_f:
        fastavro.writer(out_f, schema, records)


def combine_sources(source_names: list[str]):
    """
    Function to combine avro files for a given night.

    :return: None
    """
    candidate_dirs = list(AVRO_DIR.glob("*/"))
    candidate_dirs = [x for x in candidate_dirs if x.is_dir() & (x.name in source_names)]

    records = []

    logger.info(f"Found {len(candidate_dirs)} candidate directories")

    missing = [x for x in source_names if x not in [d.name for d in candidate_dirs]]

    if missing:
        logger.warning(f"Missing {len(missing)} candidate directories: {missing}")

    for candidate_dir in sorted(candidate_dirs):
        avro_files = list(candidate_dir.glob("*.avro"))
        for avro_path in avro_files:
            with open(avro_path, "rb") as avro_f:
                avro_reader = reader(avro_f)
                for record in avro_reader:
                    records.append(record)

    logging.info(f"Found {len(records)} individual entries")

    df = flatten_records(records)
    return df


def combine_reals():
    """
    Function to combine avro files for real sources.
    """
    real_df = get_class_from_google(real=True)
    df = combine_sources(real_df["#Winter name"].tolist())
    logging.info(f"Saving to {real_path}")
    df.to_parquet(real_path)


def combine_bogus():
    """
    Function to combine avro files for bogus sources.
    """
    real_df = get_class_from_google(real=False)
    df = combine_sources(real_df["#Winter name"].tolist())
    logging.info(f"Saving to {real_path}")
    df.to_parquet(bogus_path)
