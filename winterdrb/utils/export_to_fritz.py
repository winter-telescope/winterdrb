"""
Module to export WINTER sources to SkyPortal
"""
import logging
import sys

from mirar.data import Dataset, ImageBatch, SourceBatch
from mirar.pipelines.winter import WINTERPipeline
from mirar.processors.skyportal import SkyportalCandidateUploader, SkyportalClient
from mirar.processors.sources import CustomSourceTableModifier, SourceLoader

from winterdrb.database.query import get_night_db_entries


def export_reals(night: str | int):
    """
    Function to export real WINTER sources to SkyPortal

    :param night: Night to export
    :return: None
    """

    df = get_night_db_entries(night, include_classified=True)

    real_mask = df["humanclass"] == 1

    reals = df[real_mask]["objectid"].tolist()

    print(f"Having the following reals: {reals}")

    def select_real_sources(source_batch: SourceBatch) -> SourceBatch:
        """
        Function to select only human-tagged Real sources

        :param source_batch: Source batch
        :return: updated batch
        """

        new_batch = SourceBatch([])

        for source in source_batch:
            src_df = source.get_data()

            mask = src_df["objectid"].isin(reals)

            filtered_df = src_df[mask]

            source.set_data(filtered_df)
            new_batch.append(source)

        print(f"Have {len(new_batch)} sources")

        return new_batch

    export_real_pipe = [
        SourceLoader(input_dir_name="preskyportal"),
        CustomSourceTableModifier(modifier_function=select_real_sources),
        SkyportalCandidateUploader(
            origin="WINTERTEST",
            group_ids=[1431],
            fritz_filter_id=1130,
            instrument_id=1087,
            stream_id=1005,
            update_thumbnails=True,
            skyportal_client=SkyportalClient(base_url="https://fritz.science/api/"),
        ),
    ]

    log = logging.getLogger("mirar")

    handler = logging.StreamHandler(sys.stdout)

    formatter = logging.Formatter(
        "%(name)s [l %(lineno)d] - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    log.addHandler(handler)
    log.setLevel("INFO")

    new_key = "export_real"

    pipeline = WINTERPipeline(night=night, selected_configurations=["reduce"])
    pipeline.add_configuration(
        configuration_name=new_key, configuration=export_real_pipe
    )
    pipeline.set_configuration(new_key)

    pipeline.reduce_images(
        Dataset([ImageBatch()]),
        selected_configurations=[new_key],
        catch_all_errors=True,
    )
