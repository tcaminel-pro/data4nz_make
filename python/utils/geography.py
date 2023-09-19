"""
  Utilities related to geographical areas

  Copyright (C) 2023 Eviden. All rights reserved
"""

from constructive_geometries import Geomatcher
import country_converter as coco
from collections import Iterable


def flatten(xs: list) -> Iterable:
    """
    Flaten a list, even having different levels of nesting.
    taken from https://stackoverflow.com/questions/2158395/flatten-an-irregular-arbitrarily-nested-list-of-lists
    """
    for x in xs:
        if isinstance(x, Iterable) and not isinstance(x, (str, bytes)):
            yield from flatten(x)
        else:
            yield x


def get_enclosing_geo_from_country(country: str) -> list:
    """
    Return the list of country enclosing geographies.  The topology is the one used by
    the EcoInvent database.
    Include the country itself, and RoW (Rest of the World)
    Args:
        country : country name or ISO code
    """
    geo = coco.convert(names=country, to="ISO2")
    g = Geomatcher().within(geo, biggest_first=True)
    return [a for a in flatten(g) if a != "ecoinvent"] + ["RoW"]


def get_countries_within_geo(geo: str) -> list:
    l = Geomatcher().within(geo, biggest_first=True)
    r = {s for s in l if type(s) is str}
    r.discard("GLO")
    return r
