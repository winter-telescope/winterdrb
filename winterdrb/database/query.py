"""
Module for querying the database
"""
import pandas as pd
from mirar.database.constraints import DBQueryConstraints
from mirar.database.transactions import select_from_table

from winterdrb.models import RealBogus


def get_night_db_entries(night, include_classified: bool = False,
                         max_num_results: int = None) -> pd.DataFrame:
    """
    Get all entries from the night database for a given night

    :param night: Night to get entries for
    :param include_classified: Boolean to include classified entries
    :param max_num_results: Limit the number of entries returned
    :return: Datframe with entries
    """
    constraint = DBQueryConstraints("night", night, "=")

    if not include_classified:
        constraint.add_constraint("humanclass", 1, "<")

    return select_from_table(constraint, RealBogus.sql_model,
                             max_num_results=max_num_results)

