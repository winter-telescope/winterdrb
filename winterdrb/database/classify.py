"""
This module contains functions for classifying sources in the database.
"""
from mirar.database.constraints import DBQueryConstraints
from mirar.database.transactions import _update_database_entry

from winterdrb.models import RB_CLASS, RealBogus


def classify_source(candid, new_classification: int):
    """
    Update the classification of a source in the database

    :param candid: Candidate ID
    :param new_classification: Integer classification
    :return: None
    """

    assert new_classification in RB_CLASS

    con = DBQueryConstraints("candid", candid, "=")

    update_dict = {"humanclass": new_classification}

    _update_database_entry(update_dict, con, RealBogus.sql_model)
