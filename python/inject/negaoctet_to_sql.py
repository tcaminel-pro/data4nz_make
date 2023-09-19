"""
    Convert NegaOctet 2022 Excel file to SQL tables
  
    Copyright (C) 2023 Eviden. All rights reserved
"""


import sys
import pandas as pd
import sqlalchemy as sa
import sqlalchemy_utils as sau
from pathlib import Path
from sqlalchemy.engine.base import Engine
from sqlalchemy.types import Float
import pg8000  # used by the PostgresSQL driver we use
import logging
from math import isnan

if (cdir := str(Path.cwd())) not in sys.path:
    sys.path.append(cdir)

from python.utils.pandas_utils import rename_duplicated_index
from python.utils.dbt_utils import get_database_url_from_dbt_profile
from python.utils.string_utils import trim_and_replace_non_ascii

DB_PREFIX = "NO22_"

DBT_PROFILE = "emission_factors_make"
SCHEMA = "ref_data"
BASE_DIR = Path(
    "/mnt/c/Users/A184094/OneDrive - Atos/data4nz_copy/datasets/emission_factors"
)  # synched with SP


def excel_to_db(file: Path, db_eng: Engine):
    """
    Convert Negaocted Excel file (2022) to SQL tables

    Arg:
    - File : path to the Excel file
    - dg_eng : SqlAlchemy SQL engine object
    - methods_used : LCA methods used, ie which data to extract amoong the many methods in EcoInvent
    """
    logging.info(f"reading Excel {file=} ...")
    df = pd.read_excel(file, header=None)
    values_table_name = f"{DB_PREFIX}Values"
    db_eng.execute(f"""DROP TABLE IF EXISTS "{values_table_name}" CASCADE;""")
    logging.info("Start processing...")
    methods = set()
    categories = set()
    indicators = set()
    units = set()
    for col_id, col_content in df[:3].iteritems():
        if col_id < 14:
            continue
        categories.add(str(col_content[0]))
        indicators.add(col_content[1])
        units.add(col_content[2].partition(" ")[2])
    indicators = sorted(indicators)
    units = sorted(units)
    categories.remove("nan")
    categories = sorted(categories)

    pd.DataFrame(categories, columns=["category"]).to_sql(
        f"{DB_PREFIX}Categories",
        db_eng,
        index=True,
        if_exists="replace",
        schema=SCHEMA,
    )
    pd.DataFrame(indicators, columns=["indicator"]).to_sql(
        f"{DB_PREFIX}Indicators",
        db_eng,
        index=True,
        if_exists="replace",
        schema=SCHEMA,
    )
    pd.DataFrame(units, columns=["unit"]).to_sql(
        f"{DB_PREFIX}Units",
        db_eng,
        index=True,
        if_exists="replace",
        schema=SCHEMA,
    )

    df_product = df[range(10)]
    df_product.columns = df_product.iloc[0]
    df_product = df_product.drop([0, 1, 2])

    df_param = df[range(10, 14)]
    df_param.columns = df_param.iloc[1]
    df_param = df_param.drop([0, 1, 2])

    df_product[df_param.columns] = df_param[df_param.columns]

    dff = df_product.rename(
        columns={"Manufacturing ecobilan": "manuf_bilan"}, errors="raise"
    )
    dff = dff.set_index("manuf_bilan")
    dff = rename_duplicated_index(dff)

    # replace dash and possible other non ascii characters
    dff["Name"] = dff["Name"].apply(lambda x: trim_and_replace_non_ascii(x))
    dff.to_sql(
        f"{DB_PREFIX}Products",
        db_eng,
        index=True,
        index_label="manuf_bilan",
        if_exists="replace",
        schema=SCHEMA,
    )

    logging.info("looping over rows..")
    loop = 0
    category = None
    for col_id, col_content in df.iteritems():
        if col_id < 14:
            continue
        df_col = pd.DataFrame()
        if str(col_content[0]) != "nan":
            category = str(col_content[0])
        unit = col_content[2].partition(" ")[2]
        df_col["manuf_bilan"] = dff.index
        df_col["category_id"] = categories.index(category)
        df_col["indicator_id"] = indicators.index(col_content[1])
        df_col["unit_id"] = units.index(unit)
        df_col["value"] = col_content[3:].reset_index(drop=True)
        df_col.set_index("manuf_bilan", inplace=True)
        df_col.to_sql(
            values_table_name,
            db_eng,
            index=True,
            index_label="manuf_bilan",
            dtype={"value": Float()},
            if_exists="replace" if loop == 0 else "append",
            schema=SCHEMA,
        )
        loop = loop + 1
    logging.info("Done")
    # print(df_columns_to_sql(dff))


def main():
    # get database connection URL from DBT profile  (~/profile.yaml)
    db_url = get_database_url_from_dbt_profile(DBT_PROFILE)
    db_eng = sa.create_engine(db_url)

    negaoctet_db = "20220614083947_negaoctet-liste-des-donnees_externe_220211.xlsx"
    excel_to_db(Path(BASE_DIR, negaoctet_db), db_eng)


logging.basicConfig(level=logging.DEBUG, format="%(levelname)s:%(message)s")
main()
