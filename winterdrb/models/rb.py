"""
Models for the 'realbogus' table
"""
import logging
from typing import ClassVar

from mirar.database.base_model import BaseDB
from mirar.pipelines.winter.models.base_model import WinterBase
from pydantic import Field
from sqlalchemy import VARCHAR, BigInteger, Column, Integer

logger = logging.getLogger(__name__)


class RealBogusTable(WinterBase):  # pylint: disable=too-few-public-methods
    """
    RB table in database
    """

    __tablename__ = "realbogus"
    __table_args__ = {"extend_existing": True}

    # Core fields
    candid = Column(
        BigInteger,
        unique=True,
        primary_key=True,
    )
    night = Column(VARCHAR(8), nullable=False, unique=False)
    objectid = Column(VARCHAR(40), nullable=False, unique=False)
    pdfpath = Column(VARCHAR(255), nullable=False, unique=False)
    avropath = Column(VARCHAR(255), nullable=False, unique=False)
    humanclass = Column(Integer, nullable=False, unique=False)
    comment = Column(VARCHAR(255), nullable=True, unique=False)


class RealBogus(BaseDB):
    """
    A pydantic model for a real/bogus database entry
    """

    sql_model: ClassVar = RealBogusTable

    candid: int = Field(default=0)
    night: int = Field(ge=0)
    objectid: str = Field()
    pdfpath: str = Field()
    avropath: str = Field()
    humanclass: int = Field(default=-1, ge=-1)
    comment: str = Field(default=None, max_length=255)
