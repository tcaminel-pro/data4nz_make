"""
Database (mostly tested on PostgreSQL) related utilities

Copyright (C) 2023 Eviden. All rights reserved
"""

from loguru import logger
import sqlalchemy as sa
from pathlib import Path
from sqlalchemy import inspect
import sqlparse
from sqlalchemy.engine.base import Engine
import pandas as pd


def change_db_schema(
    db_url: str, source_schema: str, target_schema: str, table_start: str
):
    """
    Change the schemas of tables in PostgresSQL
    Args:
        db_url : URL of the PG database
        source_schema, target_schema : name of schemas in the database
        table_start : first letters of the tables to move
    """
    engine = sa.create_engine(db_url)
    inspector = inspect(engine)

    logger.info(
        f"changing schema of {table_start}* from {source_schema} to {target_schema}"
    )
    for table_name in inspector.get_table_names(schema=source_schema):
        if table_name.startswith(table_start):
            sql = f"""ALTER TABLE {source_schema}."{table_name}" SET SCHEMA {target_schema}"""
            logger.debug(sql)
            with engine.begin() as conn:
                conn.execute(sql)
                pass


def add_full_text_index(
    db_url, schema, table_name, columns_name_weight, config="english"
):
    """
    Add  columns and an index for full text search
    Args:
        db_url: url of the databases
        schema : schema where the table is
        table_name : table to index
        columns_name_weight : list of tupple with:
            - weight of the column
            - column name (between double quote) or expression (like split_part("textfield",...))
        config : search config name

    see https://www.postgresql.org/docs/current/textsearch-tables.html
    """

    l = [
        f"""setweight(to_tsvector('{config}', coalesce({column}, '')), '{weight}')"""
        for (weight, column) in columns_name_weight
    ]
    columns_list_sql = " || ' ' || ".join(l)
    sql = [
        f"""ALTER TABLE {schema}."{table_name}" DROP COLUMN IF EXISTS text_index_col CASCADE;""",
        f"""
         ALTER TABLE {schema}."{table_name}" 
         ADD COLUMN text_index_col  tsvector
         GENERATED ALWAYS AS ( {columns_list_sql} ) STORED;""",
        f"""CREATE INDEX "search_{schema}_{table_name}_idx" ON {schema}."{table_name}"
            USING GIN (text_index_col);""",
    ]
    # todo for better perf: consider RUM https://github.com/postgrespro/rum
    engine = sa.create_engine(db_url)
    with engine.begin() as conn:
        for s in sql:
            print(sqlparse.format(s, reindent=True, keyword_case="upper"))
            conn.execute(s)


def postgres_to_parquet(
    table_name: str, db_schema: str, pg_url: str, parquet_dir: Path
):
    """
    Copy tables in Postgres to Parquet files, using DuckDB
    """

    eng = sa.create_engine(
        "duckdb:///:memory:",
        connect_args={"preload_extensions": ["https", "postgres_scanner"]},
    )
    pg_url = pg_url.replace("+pg8000", "")
    if not parquet_dir.exists():
        logger.info("create dir: {parquet_dir}")
        Path.mkdir(parquet_dir)

    output = Path(parquet_dir, table_name).with_suffix(".parquet")
    logger.info(f"Copy table {table_name} to {output}")
    pg_sql = f"""
        SELECT * FROM POSTGRES_SCAN('{pg_url}', {db_schema}, {table_name})"""
    sql_parquet = f"""COPY ({pg_sql}) TO '{output}' (FORMAT PARQUET,CODEC ZSTD) """
    try:
        eng.execute(sql_parquet)
    except Exception as ex:
        # logger.exception("SQL issue...", backtrace=False, diagnose=False)
        logger.error(f"Exception {ex}")


def get_db_info_from_url(dburl: str) -> dict:
    r = dict()
    if dburl.startswith("postgresql"):
        r["type"] = "postgresql"
    elif dburl.startswith("postgresql"):
        r["type"] = "duckdb"
        r["path"] = dburl.partition("///")[2]
    else:
        raise ValueError(f"cant't analyse URL: {dburl} ")
    return r


def table_exists(dbengine: Engine, table_name: str) -> bool:
    """
    Test if a SQL table exists
    """
    return sa.inspect(dbengine).has_table(table_name)


def getTablesFromDB(dbengine: Engine, schema="public") -> list[str]:
    """
    return the list of tables in a DB
    """
    return [sa.inspect(dbengine).get_table_names(schema=schema)]


def copyTablesBetweenBD(
    src: Engine, dest: Engine, always: list = [], regexp: str = ".*"
):
    """
    usage:
    db_url_pg_local = "postgresql://postgres:tcladmin@localhost:5432/EcoData"
    db_url_pg_cloud = "postgresql://ecoactor_admin:xxxx@localhost:5436/EcoData_v1"
    src = sa.create_engine(db_url_pg_local)
    dst = sa.create_engine(db_url_pg_cloud)

    copyTablesBetweenBD(src, dst, always ="CDP2021_QuestionsList", subset_to_copy = None )

    """

    logger.info(f"copy tables from {src.url} to {dest.url}")
    src_inspect = sa.inspect(src)
    src_tables = src_inspect.get_table_names()
    dest_tables = sa.inspect(dest).get_table_names()  # schema='public' ?
    src_tables_filtered = (t for t in src_tables if re.match(regexp, t))
    src_tables_set = set(src_tables_filtered).difference(dest_tables).union(always)
    logger.debug(f"{src_tables_set=}")
    for table in src_tables_set:
        idx = src_inspect.get_indexes(table)
        idx_col = [e["column_names"] for e in idx][0] if idx else None
        df = pd.read_sql(f"""SELECT * from "{table}" ;  """, src, index_col=idx_col)
        df.to_sql(table, dest, if_exists="replace")
