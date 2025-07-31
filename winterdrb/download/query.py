from mirar.database.constraints import DBQueryConstraints
from mirar.database.transactions import select_from_table
from mirar.pipelines.winter.models import Candidate, Diff
import pandas as pd


def get_candidate_entries(
    name: str, max_num_results: int = None
) -> pd.DataFrame:
    """
    Get all entries from the night database for a given night

    :param name: Name of the candidate to get entries for
    :param max_num_results: Limit the number of entries returned
    :return: Dataframe with entries
    """
    constraint = DBQueryConstraints("objectid", name, "=")

    res = select_from_table(
        constraint, Candidate.sql_model, max_num_results=max_num_results
    )
    return res

def get_diff_entries(
    diffid: int, max_num_results: int = None
) -> pd.DataFrame:
    """
    Get all entries from the night database for a given night

    :param diffid: Diff ID to get entries for
    :param max_num_results: Limit the number of entries returned
    :return: Dataframe with entries
    """
    constraint = DBQueryConstraints("diffid", diffid, "=")

    res = select_from_table(
        constraint, Diff.sql_model, max_num_results=max_num_results
    )
    return res