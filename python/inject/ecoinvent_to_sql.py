"""
Convert EcoInvent Excel file to SQL tables.
Tested with EI 3.8 and EI 3.9

Copyright (C) 2023 Eviden. All rights reserved
"""

import sys
from typing import cast
from devtools import debug
import pandas as pd
import sqlalchemy as sa
from pathlib import Path
from sqlalchemy.engine.base import Engine
from sqlalchemy.types import Float
from loguru import logger


if (cdir := str(Path.cwd())) not in sys.path:
    debug(cdir)
    sys.path.append(cdir)

debug(sys.path)


from python.utils.excel_utils import num_to_xls_col_name
from python.utils.dbt_utils import get_database_url_from_dbt_profile


DBT_PROFILE = "emission_factors_make"
SCHEMA = "ref_data"
BASE_DIR = Path(
    "/mnt/c/Users/A184094/OneDrive - Atos/data4nz_copy/datasets/emission_factors"
)  # synched with SP

# FILE_TO_PROCESS = "Cut-off Cumulative LCIA v3.9 EF3.1 - SHORTEN.xlsx"  # for test
FILE_TO_PROCESS = "Cut-off Cumulative LCIA v3.9 EF3.1.xlsx"  # for test
METHODS = {"EF v3.1 no LT"}
PREFIX = "EI3.9_"

assert BASE_DIR.exists
assert (BASE_DIR / FILE_TO_PROCESS).exists


def excel_to_db(file: Path, db_eng: Engine, schema: str, methods_used: set[str]):
    """
    Convert EcoInvent Excel file to SQL tables

    Arg:
    - File : path to the Excel file
    - dg_eng : SqlAlchemy SQL engine object
    - methods_used : LCA methods used, ie which data to extract amoong the many methods in EcoInvent
    """
    logger.info(f"reading Excel {file} ; That might takes a few minutes...")
    df = pd.read_excel(file, sheet_name="LCIA", header=None)
    values_table_name = "EI3.9_Values"
    with db_eng.connect() as conn: 
        conn.execute(f"""DROP TABLE IF EXISTS "{schema}.{values_table_name}" CASCADE;""")
    logger.info("Start processing...")
    methods = set()
    categories = set()
    indicators = set()
    units = set()
    for col_id, col_content in df[:4].items():
        if col_id <= 5:
            continue
        methods.add(col_content[0])
        categories.add(col_content[1])
        indicators.add(col_content[2])
        units.add(col_content[3])
    methods = sorted(methods)
    categories = sorted(categories)
    indicators = sorted(indicators)
    units = sorted(units)
    pd.DataFrame(methods, columns=["method"]).to_sql(
        f"{PREFIX}Methods", db_eng, schema=schema, index=True, if_exists="replace"
    )
    pd.DataFrame(categories, columns=["category"]).to_sql(
        f"{PREFIX}Categories", db_eng, schema=schema, index=True, if_exists="replace"
    )
    pd.DataFrame(indicators, columns=["indicator"]).to_sql(
        f"{PREFIX}Indicator", db_eng, schema=schema, index=True, if_exists="replace"
    )
    pd.DataFrame(units, columns=["units"]).to_sql(
        f"{PREFIX}Units", db_eng, schema=schema, index=True, if_exists="replace"
    )

    df_product = cast(pd.DataFrame, df[range(6)].drop([0, 1, 2]))  # cast to make PyLint happy
    df_product.columns = df_product.iloc[0]
    df_product.rename(
        columns={"Activity UUID_Product UUID": "uuids"}, inplace=True, errors="raise"
    )
    df_product.set_index("uuids", inplace=True)
    df_product.drop(["Activity UUID_Product UUID"], inplace=True)
    logger.info(f"save {PREFIX}Activities - col = {list(df_product.columns)}")
    df_product.to_sql(
        f"{PREFIX}Activities", db_eng, schema=schema, index=True, if_exists="replace"
    )

    logger.info("looping over rows..")
    loop = 0
    for col_id, col_content in df.items():
        if col_id <= 5:
            continue
        df_col = pd.DataFrame()
        method = col_content[0]
        if method in methods_used:
            print(col_id, end=" ", flush=True)
            df_col["value"] = col_content[4:].reset_index(drop=True)
            df_col["uuids"] = df_product.index
            df_col["method_id"] = methods.index(method)
            df_col["category_id"] = categories.index(col_content[1])
            df_col["indicator_id"] = indicators.index(col_content[2])
            df_col["unit_id"] = units.index(col_content[3])
            df_col["excel_column"] = num_to_xls_col_name(col_id + 1)
            df_col.set_index("uuids", inplace=True)
            try:
                df_col.to_sql(
                    values_table_name,
                    db_eng,
                    schema=schema,
                    index=True,
                    dtype={"value": Float()},
                    if_exists="replace" if loop == 0 else "append",
                )
            except Exception as ex:
                logger.exception(ex)
                logger.error(f"in:{df_col}")
            loop = loop + 1
    logger.info("Done")


def main():
    # get database connection URL from DBT profile  (~/profile.yaml)
    db_url = get_database_url_from_dbt_profile(DBT_PROFILE)
    db_eng = sa.create_engine(db_url)

    logger.info(f"target DB : {db_eng}")

    # LCA methods used by AcoAct consultants
    excel_to_db(BASE_DIR / FILE_TO_PROCESS, db_eng, SCHEMA, METHODS)


main()
