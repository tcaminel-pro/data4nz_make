"""
CLI commands leveraging data in a dbt model:
    - inject "simple" datasets in CSV or XLS format
    - export to parquet
    - generate SQL schema
    - Change schema


Copyright (C) 2023 Eviden. All rights reserved
"""
import os
from pathlib import Path
from devtools import debug

import pandas as pd
import sys
from loguru import logger
import typer
import duckdb

import sys

if (cdir := str(Path.cwd())) not in sys.path:
    sys.path.append(cdir)
sys.path.insert(0, '..')

from python.utils.dbt_utils import (
    get_database_url_from_dbt_profile,
    read_dbt_model,
)
from python.utils.excel_utils import RefData, inject_table_from_file
from python.utils.sql_utils import change_db_schema


DBT_PROFILE = "emission_factors_make"
SCHEMA = "ref_data"
DATASETS_DIR = Path(os.environ["DATA4NZ_DATASETS"])
PROJECT_FOLDER = Path(os.environ["DATA4NZ_MAKE_FOLDER"])
PARQUET_BASE = Path(os.environ["DATA4NZ_DATALAKE"])

assert DATASETS_DIR.exists and PROJECT_FOLDER.exists and PARQUET_BASE.exists

####
####  COMMANDS #####
####
app = typer.Typer()

DEFAULT_DBT_SOURCES = PROJECT_FOLDER / "dbt/models/sources.yml"   
DEFAULT_DBT_MODELS = PROJECT_FOLDER / "dbt/models/models.yml"


def postgres_to_parquet(
    pg_url: str, table: str, schema: str, parquet_file: Path, force: bool
):
    """
    Copy tables from PostgreSQL to Parquet files

    Args:
    pg_url: URL to  the database
    table : name of the table in the db
    schema: name of the schame where the tabble stand in the database
    parquet_file: full Path to the Parquet file to create (with file name)
    force: recreate the Parquet file is it exists

    """
    con = duckdb.connect()  # in memory
    con.execute("INSTALL postgres;")
    con.execute("LOAD postgres; ")

    pg_url = pg_url.replace("+pg8000", "")
    if parquet_file.exists() and not force:
        logger.info(
            f"Parquet file exists: '{parquet_file}'; use --force option to overwrite it"
        )
    else:
        pg_sql = f"""
                SELECT  * 
                FROM POSTGRES_SCAN('{pg_url}', '{schema}', '{table}')"""

        logger.info(f"Create Parquet file '{parquet_file}' from '{schema}.{table}' ")
        sql_parquet = f"""
                COPY ({pg_sql}) TO '{parquet_file}' (FORMAT PARQUET,CODEC ZSTD) """
        try:
            con.execute(sql_parquet)
        except Exception as ex:
            logger.error(f"Can't copy file : {ex}")
        con.close()


@app.command("sql-schema")
def print_sql_schema(table: str, dbt_sources: Path = DEFAULT_DBT_SOURCES):
    """
    print SQL SELECT clause renaming the tables column names to be "postgres friendly" (lower case, ascii,)
    """
    # ex: python python/inject/cmd_from_dbt_model.py sql-schema EI3.9_Elementary_Exchangess --dbt-sources dbt/emission_factors/models/sources.yml
    
    from python.utils.pandas_utils import df_columns_to_sql

    dbt_model = read_dbt_model(dbt_sources)
    source = dbt_model["sources"][0]
    schema = source["schema"]
    tables = source["tables"]

    found = False
    db_url_postgres = get_database_url_from_dbt_profile(DBT_PROFILE)
    for table_name in tables:
        table_name = table_name["name"]
        if table_name == table:
            found = True
            df = pd.read_sql(
                f"""select * from {schema}."{table_name}" limit 10""",
                db_url_postgres,
            )
            print(df_columns_to_sql(df, f'{schema}."{table_name}'))
    if not found:
        logger.warning(f"table not found: {table}")


@app.command("gen-tables")
def gen_tables_from_dbt_sources(dbt_sources: Path, replace_if_exists: bool = False):
    """
    Creata tables from sources in dbt model YAML file (with sources).
    Source must:
    - have the dbt property 'loader' set to "simple_files_to_sql.py"
    - have a additional node 'meta' with
        - property 'origin' : path to the Excel file
        - optional: import_method (PANDAS or COPY),
        - optional: read_args  (passed to pandas.read_excel argument)

    usage example:
        python python/inject/cmd_from_dbt_model.py gen-tables dbt/models/sources.yml
    """

    dbt_model = read_dbt_model(dbt_sources)
    source = dbt_model["sources"][0]
    schema = source["schema"]
    tables = source["tables"]

    db_url_postgres = get_database_url_from_dbt_profile(DBT_PROFILE)
    for table in tables:
        if (
            (meta := table.get("meta"))
            and source["loader"] == "cmd_from_dbt_model.py"
            and (origin := meta.get("origin"))
        ):
            ref = RefData(
                table_name=table["name"],
                path=origin,
                desc=str(table["description"]),
                db_schema=schema,
                read_args=meta.get("read_args") or {},  # type: ignore
                method=meta.get("import_method") or "PANDAS",
            )
            inject_table_from_file(
                DATASETS_DIR,
                ref,
                db_url_postgres,
                replace_if_exists,
            )


@app.command("gen-parquet")
def gen_parquet_hardcoded():
    # ex use:
    # export PYTHONPATH=./python/
    # python python/inject/cmd_from_dbt_model.py gen-parquet

    TARGET_PATH = PARQUET_BASE / "parquet/emission_factors"
    VERSION = "2a"

    db_url_postgres = get_database_url_from_dbt_profile(DBT_PROFILE)

    tables = ["ef_merged_items", "ef_merged_values"]
    schema = "ref_data"
    for table_name in tables:
        postgres_to_parquet(
            pg_url=db_url_postgres,
            table=table_name,
            schema=schema,
            parquet_file=TARGET_PATH / f"{table_name}_{VERSION}.parquet",
            force=True,
        )


def gen_parquet_from_dbt_models(dbt_models: Path, replace_if_exists: bool = False):
    """
    Creata tables from sources in dbt model YAML file (with sources).
    Source must:
    - have a additional node 'meta' with to_parquet
        -

    usage example:
        python python/inject/simple_files_to_sql.py
    """

    db_url_postgres = get_database_url_from_dbt_profile(DBT_PROFILE)

    if 0:
        dbt_model = read_dbt_model(dbt_models)

        models = dbt_model["models"]
        debug(models)

        db_url_postgres = get_database_url_from_dbt_profile(DBT_PROFILE)
        for table in models:
            if (meta := table.get("meta")) and (to_parquet := meta.get("to_parquet")):
                table_name = table["name"]
                logger.info(f"Copy {table_name} to {to_parquet}")


@app.command("change-schema")
def change_schema(table_start: str, source_schema: str, target_schema: str):
    """
    Change the schema of tables in Postgres
    """
    db_url_postgres = get_database_url_from_dbt_profile("emission_factors_make")
    change_db_schema(db_url_postgres, source_schema, target_schema, table_start)


if __name__ == "__main__":
    logger.remove()
    logger.add(
        sys.stderr, format="{level} - {time:HH:mm:ss}: {message} [{function}'{line})]"
    )

    db_url_postgres = get_database_url_from_dbt_profile(DBT_PROFILE)

    if len(sys.argv) > 1:
        app()
    else:
        logger.warning("**** FOR TEST ******")

    logger.debug("Done")
