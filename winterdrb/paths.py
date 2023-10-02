"""
Module for path constants and functions.
"""
import os
from pathlib import Path

INPUT_DATA_DIR = os.getenv('INPUT_DATA_DIR', None)
BASE_DATA_DIR = os.getenv("BASE_DATA_DIR", None)

if INPUT_DATA_DIR is None:
    raise ValueError("INPUT_DATA_DIR not set, set it using "
                     "`export INPUT_DATA_DIR=/path/to/data`")

if BASE_DATA_DIR is None:
    raise ValueError("BASE_DATA_DIR not set, set it using "
                     "`export BASE_DATA_DIR=/path/to/data`")

INPUT_DATA_DIR = Path(INPUT_DATA_DIR)
BASE_DATA_DIR = Path(BASE_DATA_DIR)


def get_raw_avro_dir(night: str | int, data_dir: Path = INPUT_DATA_DIR) -> Path:
    """
    Function to get the raw avro directory for a given night.

    :param night: Night to get the raw avro directory for.
    :param data_dir: Directory to look for data in.
    :return: Directory path.
    """
    avro_dir = data_dir.joinpath(f"{night}/avro/")
    return avro_dir


def get_combined_avro_path(night: str | int) -> Path:
    """
    Function to get the combined avro file for a given night.

    :param night: Night to get the combined avro file for.
    :return: File path.
    """
    avro_dir = get_raw_avro_dir(night, data_dir=BASE_DATA_DIR)
    avro_out_dir = avro_dir.parent.joinpath("winter_avro_combine")
    avro_out_dir.mkdir(parents=True, exist_ok=True)
    out_path = avro_out_dir.joinpath("combined.avro")
    return out_path


def get_pdf_path(night: str | int) -> Path:
    """
    Function to get the combined candidate pdf for a given night.

    :param night: Night to get the combined avro file for.
    :return: File path.
    """
    avro_dir = get_raw_avro_dir(night, data_dir=BASE_DATA_DIR)
    pdf_out_dir = avro_dir.parent.joinpath("winter_candidate_pdf")
    pdf_out_dir.mkdir(parents=True, exist_ok=True)
    out_path = pdf_out_dir.joinpath("combined.pdf")
    return out_path


def get_single_png_path(night: str | int, candid: str | int) -> Path:
    """
    Function to get the combined candidate pdf for a given night.

    :param night: Night to get the combined avro file for.
    :param candid: Candidate ID to get the png for.
    :return: Single page png path.
    """
    outdir = (get_pdf_path(night)).parent.parent.joinpath("png_single")
    outdir.mkdir(parents=True, exist_ok=True)
    return outdir.joinpath(f"{candid}.png")
