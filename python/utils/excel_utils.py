"""
Utilities to deal with Excel files.

 - Copy CSV or XLS files from a filesystem to a table in a SQL database. 
   Support different file formats, and different methods
   
 - Find Excel column name from a given column number

Copyright (C) 2023 Eviden. All rights reserved

"""
from string import ascii_uppercase
from zipfile import ZipFile
from bs4 import BeautifulSoup
from pydantic.dataclasses import dataclass
from pydantic import Field
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine.base import Engine
from pathlib import Path
import time
from loguru import logger


@dataclass
class RefData:
    table_name: str  # name of the generated table
    path: str  # relative path to the source file
    desc: str = ""  # description
    db_schema: str = "ref_data"  # schema name in the targe db

    read_args: dict = Field(
        default_factory=dict
    )  # arguments to ba added in Pandas 'read_' function
    write_args: dict = Field(
        default_factory=dict
    )  # arguments to ba added in Pandas 'write_' function
    method: str = "PANDAS"  # 'PANDAS' or 'COPY' (for very large CSV files)


def create_table_trough_pandas(dir: Path, ref: RefData, dbeng: Engine):
    """
    Create a table in the database from a file  by using Pandas.
    Works fine for tables with less than 200000 rows and not too many columns
    Typically, importing OECD GDP data (240000 rows) takes 2 to 4 minutes with PG in local
    """
    df = pd.DataFrame()
    source_fn = Path(dir, ref.path)
    print(f"import {source_fn}")
    t = time.process_time()

    if source_fn.suffix == ".csv":
        df = pd.read_csv(source_fn, **ref.read_args, engine="c")
    elif source_fn.suffix == ".xlsx" or source_fn.suffix == ".xls":
        df = pd.read_excel(source_fn, **ref.read_args)
    else:
        logger.error(f"unknown file extension for file : {source_fn}")
    if not df.empty:
        logger.info(
            f"save to table {ref.db_schema}.'{ref.table_name}' ({df.shape[0]} rows)"
        )
        for c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="ignore")
        # We need to drop table with option "CASCADE" (in PosgreSQL)
        # otherwise the DB complains because of linked views
        # (but that's OK because the views are generated)
        with dbeng.connect() as conn:
            conn.execute(
                f"""DROP TABLE IF EXISTS {ref.db_schema}."{ref.table_name}" CASCADE;"""
            )
        has_index = bool(ref.read_args.get("index_col"))
        df.to_sql(
            ref.table_name,
            dbeng,
            schema=ref.db_schema,
            if_exists="replace",
            index=has_index,
            **ref.write_args,
        )
        elapsed_time = time.process_time() - t
        logger.info(f"done; imported  in {str(elapsed_time)}")
    else:
        logger.warning("empty dataframe")


def create_table_trough_sql_copy(dir: Path, ref: RefData, dbeng: Engine):
    """
    To be implemented
    method:

    - create a tempory table with a schema, like:
        ""CREATE TABLE TEMP_ISIN_LEI {LEI VARCHAR(20), ISIN VARCHAR(12) PRIMARY KEY }""
         #eng.execute(s)
    - execute COPY https://www.postgresql.org/docs/current/sql-copy.html
      (or use alternatives)
    - Drop old table if needed and renamme the new one

    """
    source_fn = Path(dir, ref.path)
    print(f"import {source_fn}")
    print("...  NOT IMPLEMENTED")


def inject_table_from_file(
    dir: Path, ref: RefData, db_url: str, replace_if_exists=False
):
    """
    Create a list of tables

    Args:
    - dir : base directory
    - Ref: table to inject RefData
    - db_url: Alchemy URL to the database
    - load_if_exists : load the file even if the table exists
    - replace_if_exists : true if we delete the old table and create a new one
    """
    eng = sa.create_engine(db_url)
    table_exists = sa.inspect(eng).has_table(ref.table_name, schema=ref.db_schema)
    logger.info(
        f"Import {ref.db_schema}.{ref.table_name}  from {ref.path} ({table_exists=})"
    )
    if replace_if_exists == False and table_exists:
        logger.info(f"table already exists and not overwrite => skip ")
        return
    if not Path(ref.path).exists:
        logger.warning(f"file does not exists => skip ")
        return
    if ref.method == "PANDAS":
        create_table_trough_pandas(dir, ref, eng)
    elif ref.method == "COPY":
        create_table_trough_sql_copy(dir, ref, eng)
    else:
        logger.error(f"unknow import method:{ref.method} ")


def generate_dbt_schema(refs: list[RefData], with_meta=True):
    """
    Generate a YAML text to be cut and pasted in a dbt schema
    """
    print("    tables:")
    for ref in refs:
        desc = f"{ref.desc} (imported from {Path(ref.path).name}) "
        print(f"      - name: {ref.table_name}")
        print(f"        description: {ref.desc}")
        print(f"        schema: {ref.db_schema}")
        if with_meta:
            print("        meta:")
            print(f"          origin: {ref.path}")
            if ref.method != "PANDAS":
                print(f"          import_method: {ref.method}")
            if ref.read_args:
                print(f"          read_args:")
                for k, v in ref.read_args.items():
                    print(f"""            {k}: "{v}" """)


def num_to_xls_col_name(num: int) -> str:
    """
    Find Excel column name from a given column number ( 1-> 'A', 27->'AA', 28->'AB'...)
    source : https://www.geeksforgeeks.org/find-excel-column-name-given-number/
    """
    alpha = ascii_uppercase
    if num < 26:
        return alpha[num - 1]
    q, r = num // 26, num % 26
    if r == 0:
        return alpha[r - 1] if q == 1 else num_to_xls_col_name(q - 1) + alpha[r - 1]
    else:
        return num_to_xls_col_name(q) + alpha[r - 1]


def getSheetsFromExcel(file: str) -> list[str]:
    """
    Fast way to get sheets name in a XLSX file - see https://stackoverflow.com/a/57828581
    """
    with ZipFile(file) as zipped_file:
        summary = zipped_file.open(r"xl/workbook.xml").read()
        soup = BeautifulSoup(summary, "xml")
        sheets = [sheet.get("name") for sheet in soup.find_all("sheet")]
        return sheets
