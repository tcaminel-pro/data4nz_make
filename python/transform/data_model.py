"""
Module describing sbt target parser data model
"""
from enum import Enum
from enum import unique

from pydantic import BaseModel


class AlignmentData(BaseModel):
    """
    Object that represents the data model for sbt targets
    """
    company_name: str = ''
    target_type: str = ''
    scope: str = ''
    metric: str = ''
    coverage: str = ''
    reduction_ambition: float = 0.0
    emission_type: str = ''
    base_year: int = 0
    end_year: int = 0
    start_year: int = 0
    target: str = ''
    set: bool = True


@unique
class Scope(str, Enum):
    """
    Enum listing all possible matching combinations for scopes
    """
    S1 = 'S1'
    S2 = 'S2'
    S3 = 'S3'
    S123 = 'S1+S2+S3'
    S12 = 'S1+S2'
    S13 = 'S1+S3'


@unique
class Pattern(int, Enum):
    """
    Enum listing all matching patterns
    """
    First = 1
    Second = 2
    Third = 3
    Fourth = 4
    Fifth = 5
    Sixth = 6
    Seventh = 7
    Eighth = 8
    Ninth = 9
    Tenth = 10


@unique
class Scope3Categories(str, Enum):
    """
    Enum listing the different categories of scope 3
    """
    C1 = 'purchased goods and services'
    C2 = 'capital goods'
    C3 = 'fuel and energy related activities'
    C4 = 'upstream transportation and distribution'
    C5 = 'waste generated in operations'
    C6 = 'business travel'
    C7 = 'employee commuting'
    C8 = 'upstream leased assets'
    C9 = 'downstream transportation and distribution'
    C10 = 'processing of sold products'
    C11 = 'use of sold products'
    C12 = 'end of life treatment of sold products'
    C13 = 'downstream leased assets'
    C14 = 'franchises'
    C15 = 'investments'
