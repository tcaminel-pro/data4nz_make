"""
CLI commands to inject "simple" datasets in CSV or XLS format
to a SQL database 

Copyright (C) 2023 Eviden. All rights reserved
"""
from pathlib import Path
from pathlib import PureWindowsPath
import pandas as pd
import sys
from loguru import logger
import typer

if (cdir := str(Path.cwd())) not in sys.path:
    sys.path.append(cdir)
sys.path.insert(0, '..')
from python.utils.dbt_utils import get_database_url_from_dbt_profile, get_dbt_manifest
from python.utils.pandas_utils import df_columns_to_sql
from python.utils.excel_utils import RefData, inject_table_from_file
from python.utils.sql_utils import change_db_schema


#DATASETS_DIR = Path("/home/tcl/data4nz/", "datasets")
DATASETS_DIR = PureWindowsPath('C:/Repo/data4nz_make/datasets')
####
####  COMMANDS #####
####
app = typer.Typer()

dbt_manifests = [
    #Path("/home/tcl/prj/data4nz_make/dbt/organizations/target/manifest.json"),
    Path("C:/Repo/data4nz_make/dbt/emission_factors/target/manifest.json"),
]  # TODO: replace with config


def get_table_list() -> list[tuple[str, str]]:
    """
    Return a list with schema and name of known sources in dbt manifest
    """
    r = []
    for catalog_path in dbt_manifests:
        manifest_obj = get_dbt_manifest(catalog_path)
        r.extend([(s.schema_, s.name) for s in manifest_obj.sources.values()])
    return r


@app.command("sql-schema")
def print_sql_schema(table: str):
    """
    print SQL SELECT clause renaming the tables column names to be "postgres friendly" (lower case, ascii,)
    """
    sql_tables = get_table_list()
    print(f"{sql_tables=}")
    db_url_postgres = get_database_url_from_dbt_profile("emission_factors_make")
    found = False
    for schema, table_name in sql_tables:
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
def gen_tables_from_dbt_manifest(replace_if_exists: bool = True):
    """
    Creata tables from sources in dbt catalog.
    Source must:
    - have the dbt property 'loader' set to "simple_files_to_sql.py"
    - have a additional node 'meta' with
        - property 'origin' : path to the Excel file
        - optional: import_method (PANDAS or COPY),
        - optional: read_args  (passed to pandas.read_excel argument)
    """

    logger.info(f"Create tables from sources in dbt catalog {replace_if_exists=}")
    for catalog_path in dbt_manifests:
        manifest_obj = get_dbt_manifest(catalog_path)
        for _, source in manifest_obj.sources.items():
            meta = source.meta
            if (
                meta
                and source.loader == "simple_files_to_sql.py"
                and (origin := meta.get("origin"))
            ):
                db_url_postgres = get_database_url_from_dbt_profile(
                    "emission_factors_make"
                )
                ref = RefData(
                    table_name=source.name,
                    path=origin,
                    desc=str(source.description),
                    db_schema=source.schema_,
                    read_args=meta.get("read_args") or {},  # type: ignore
                    method=meta.get("import_method") or "PANDAS",
                )
                inject_table_from_file(
                    DATASETS_DIR,
                    ref,
                    db_url_postgres,
                    replace_if_exists,
                )


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

    #db_url_duckdb = get_database_url_from_dbt_profile("emission_factors_make")
    db_url_postgres = get_database_url_from_dbt_profile("emission_factors_make")
    # db_url_duckdb = get_database_url_from_dbt_profile("organizations_make")

    if len(sys.argv) > 1:
        app()
    else:
        logger.warning("**** FOR TEST ******")

    logger.debug("Done")
