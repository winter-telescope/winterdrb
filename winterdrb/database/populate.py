"""
Module for populating the database with data from a given night.
"""
import matplotlib.pyplot as plt
from tqdm import tqdm

from winterdrb.models import RealBogus
from winterdrb.paths import get_combined_avro_path, get_single_png_path
from winterdrb.plot import generate_single_page
from winterdrb.utils import get_cleaned_night_data


def populate_night(night: str | int, ann_fields: list[str],
                   skip_db_entries: bool = False) -> None:
    """
    Populate the database with the data from a given night.

    :param night: Night to populate
    :param ann_fields: Fields to annotate
    :param skip_db_entries: Skip adding to the database, only make plots
    :return: None
    """
    cut = get_cleaned_night_data(night)

    avropath = get_combined_avro_path(night)

    for _, row in tqdm(cut.iterrows(), total=len(cut)):
        savepath = get_single_png_path(night, row["candid"])

        generate_single_page(row, ann_fields=ann_fields)
        plt.savefig(savepath)
        plt.close()

        if not skip_db_entries:
            new = RealBogus(
                night=night,
                candid=row["candid"],
                objectid=row["objectid"],
                pdfpath=savepath.as_posix(),
                avropath=avropath.as_posix(),
            )
            new.insert_entry()
