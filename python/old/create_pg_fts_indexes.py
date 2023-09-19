"""
OBSOLETE CODE  - We keep it as it might be reused
"""

from pathlib import Path
import sqlalchemy as sa
from sqlalchemy.engine.base import Engine
import time
from pathlib import Path
import pg8000  # used by the PostgreSQL driver we use

from utils.dbutils import add_full_text_index
from dbt_utils import get_database_url


def add_full_text_to_ecoinvent(db_url: str):
    line_to_remove = "Product information for this product is available within the datasets that produce it."

    add_full_text_index(
        db_url,
        "ref_data",
        "EI3.8_Intermediate_Exchanges",
        [
            ("A", '"Name"'),
            ("C", f"""nullif("Product Information", '{line_to_remove}')"""),
            ("B", """split_part("CPC Classification",':',2)"""),
            ("A", '"Synonym"'),
        ],
        "english",
    )


def add_full_text_to_negaocted(db_url: str):
    add_full_text_index(
        db_url,
        "ref_data",
        "NO22_Products",
        [
            ("A", '"Name"'),
            ("B", '"Additional information"'),
        ],
        "english",
    )


def add_full_text_to_base_carbone_en_row(db_url: str):
    add_full_text_index(
        db_url,
        "public",
        "base_carbone_en_row",
        [
            ("A", '"item"'),
            ("B", '"description"'),
        ],
        "english",
    )


db_url = get_database_url("emission_factors_make")
print(db_url)
add_full_text_to_base_carbone_en_row(db_url)

# add_full_text_to_negaocted(db_url)
# add_full_text_to_ecoinvent(db_url)
