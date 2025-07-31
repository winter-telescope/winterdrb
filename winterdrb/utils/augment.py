"""
Use a night as 'background', despite it likely containqing some real candidates.
"""
from pathlib import Path
import logging

from fastavro import reader
from winterdrb.paths import AVRO_CACHE_DIR
from winterdrb.utils.combine_avro import flatten_records
from winterdrb.utils.google import get_class_from_google

logger = logging.getLogger(__name__)


def load_extra_background(n_images: int | None = 50):
    """
    Function to combine avro files for a given night.

    :return: None
    """
    avro_files = sorted(list(AVRO_CACHE_DIR.glob("*.avro")))[::-1]

    if n_images is not None:
        avro_files = avro_files[:n_images]

    real_df = get_class_from_google()
    bogus_df = get_class_from_google()
    known_sources = real_df["#Winter name"].tolist() + bogus_df["#Winter name"].tolist()

    records = []

    logger.info(f"Loading {len(avro_files)} images from cache")

    for avro_path in avro_files:
        with open(avro_path, "rb") as avro_f:
            avro_reader = reader(avro_f)
            for record in avro_reader:
                # Skip if the objectid is in the known sources
                if record["objectid"] in known_sources:
                    continue
                records.append(record)

    logging.info(f"Found {len(records)} background entries")
    df = flatten_records(records)
    return df