from fabric import Connection
from pathlib import Path
import fastavro
from fastavro import reader
import pandas as pd
import os

from winterdrb.paths import AVRO_CACHE_DIR, AVRO_DIR
from winterdrb.download.query import get_candidate_entries, get_diff_entries

import logging

logger = logging.getLogger(__name__)

def download_by_path(remote_avro_path: Path, cand_dir: Path, overwrite: bool = False):
    """
    Download an avro file from a remote path and save it to a local directory.

    :param remote_avro_path: Path to the remote avro file.
    :param cand_dir: Local directory to save the avro file.
    :param overwrite: Whether to overwrite the existing file if it exists.
    :return: None
    """
    cache_path = AVRO_CACHE_DIR / remote_avro_path.name

    if cache_path.exists() & (not overwrite):
        logger.debug(f"{cache_path} already exists")
    else:
        logger.debug(f"Saving to {cache_path}")
        with Connection(host=os.getenv("REMOTE_HOST")) as conn:
            try:
                conn.get(str(remote_avro_path), str(cache_path))
            except FileNotFoundError:
                logger.error(f"Failed to download file {remote_avro_path}. Ensure the remote path is correct.")
                raise

    out_path = cand_dir / remote_avro_path.name

    if out_path.exists() & (not overwrite):
        logger.debug(f"{out_path} already exists")
    elif cache_path.exists():
        records = []

        try:
            with open(cache_path, "rb") as avro_f:
                avro_reader = reader(avro_f)
                schema = avro_reader.writer_schema
                for record in avro_reader:
                    if record["objectid"] == cand_dir.name:
                        records.append(record)

            if len(records) > 0:
                logger.debug(f"Saving to {out_path}")
                with open(out_path, "wb") as out_f:
                    fastavro.writer(out_f, schema, records)

        except ValueError as e:
            logger.error(f"Failed to read avro file {cache_path}. Ensure the file is a valid avro file. {e}")
            cache_path.unlink()


def download_by_name(name: str):
    """
    Download avro files for a given candidate name.

    :param name: Candidate name to download avro files for.
    :return: None
    """

    df = get_candidate_entries(name)

    if df.empty:
        logger.warning(f"No candidate entries found for {name}")
        return

    diff_df = pd.concat([get_diff_entries(x) for x in list(set(df["diffid"]))]).reset_index(drop=True)
    diff_df["avropath"] = [Path(x.replace("diffs", "avro")).with_suffix(".avro") for x in diff_df["savepath"]]

    cand_dir = AVRO_DIR / name
    cand_dir.mkdir(parents=True, exist_ok=True)

    for path in diff_df["avropath"]:
        download_by_path(path, cand_dir)