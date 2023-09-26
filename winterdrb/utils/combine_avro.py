"""
Module to combine avro files for a given night.
"""
import logging

import fastavro
from fastavro import reader

from winterdrb.paths import get_combined_avro_path, get_raw_avro_dir

logger = logging.getLogger(__name__)


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
        err = f"No avro files found for {night}"
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
