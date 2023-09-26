"""
This module contains the models for the winterdrb database.
"""
from mirar.database.credentials import DB_USER
from mirar.database.setup import setup_database

from winterdrb.models.classifications import RB_CLASS
from winterdrb.models.rb import RealBogus, WinterBase

if DB_USER is not None:
    setup_database(db_base=WinterBase)
