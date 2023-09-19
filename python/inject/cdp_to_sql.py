"""
Copy CDP Excel file to SQL (tested with PostgreSQL and DuckDB)

We create mainly one table, 'q_and_a', with one row per value in the Excel file. 
The fields year, Organisation, the Excel sheet name, the row, the question, and the answert (value: numeric or string)

Also, we generate parquet files in 2 forms : partitionned (per  year and sheet), and non partitionned 

Copyright (C) 2023 Eviden. All rights reserved
"""


from pathlib import Path
import sys
import pandas as pd
from pandas.api.types import is_numeric_dtype
from loguru import logger
from pathlib import Path
import typer
import duckdb

from sqlalchemy.engine.base import Engine
import sqlalchemy as sa


if (cdir := str(Path.cwd())) not in sys.path:
    sys.path.append(cdir)
from python.utils.excel_utils import getSheetsFromExcel
from python.utils.sql_utils import postgres_to_parquet, get_db_info_from_url
from dbt_utils import get_database_url_from_dbt_profile


FILE_NAME = {
    2017: "Public Climate Change Data.xlsx",
    2018: "2018 Public Climate Change Data.xlsx",
    2019: "CDP_2019.xlsx",
    2020: "CDP_2020.xlsx",
    2021: "CDP_Climate_Change_Public_2021-values.xlsx",
    2022: "CDP climate change 2022 - public - EcoAct.xlsx",
}

COMMON_FIELDS = {  # fiedls repeated in every sheets, in CDP 2018 to 2022
    "Account number",
    "Authority types",
    "Country",
    "Country/Areas",
    "ISINs",
    "Primary Activity",
    "Primary Industry",
    "Primary Questionnaire Sector",
    "Primary Sector",
    "Primary activity",
    "Primary industry",
    "Primary questionnaire sector",
    "Primary sector",
    "Public",
    "Response received date",
    "Tickers",
    "sector",
}

REBUILD_TABLE = True  # False can be usefull for tests

PARQUET_ROW_GROUP_SIZE = 50 * 1024
# Create 100 rows groups approximatively.
# Smaller that default (40), but likely more efficient with  HTTPFS with a non-partitioned file


class CdpExcelToSQL:
    excel_file: Path  # Path to the Excel file
    year: int
    table_name: str
    eng: Engine
    dburl: str

    def __init__(self, year: int, datalake: Path, excel_file: str, db_url: str):
        """
        Args:
        - year: cdp year
        - datalake : path to the directory the excel files are.
        - excel file : name of the XLS file

        """

        datasets = datalake / "datasets" / "cdp"
        r = get_db_info_from_url(db_url)
        if r["type"] == duckdb:
            duckdb_dir = Path(r["path"])
            if not duckdb_dir.exists():
                logger.info("create dir: {duckdb_dir}")
                Path.mkdir(duckdb_dir)
            elif not duckdb_dir.is_dir():
                logger.error(f"{duckdb_dir} exists but not a directory")
            self.eng = sa.create_engine(db_url)

        elif r["type"] == "postgresql":
            self.eng = sa.create_engine(db_url)

        cdp_file = datasets / excel_file
        self.year = year
        self.excel_file = cdp_file
        self.dburl = db_url

    def get_df_for_column(
        self, df: pd.DataFrame, sheet_name: str, col: str, col_num: int
    ) -> pd.DataFrame:
        """
        Take a column from the Excel file, and create a DataFrame from it.
        Param:
        - df: dataframe created from the Excel sheet
        - sheet_name: the name of the sheet
        - col: the name of the Excel column
        - col_num: the number of the column in the Excel file (starting at 0)
        If the file has no row (ie the aswer is simple), then we take just the Organization and the  column
        Otherwise, we take also the row number

        We put numeric and string answers in different columns, and we delete rows that as "Question not applicable"
        Finay, we add a column with the year
        """

        if "row" in (cols := list(df.columns)):
            row_df = df[["Organization", "row", col]].copy()
        else:
            row_df = df[["Organization", col]].copy()
            row_df.insert(1, "row", 0)

        row_df[col] = pd.to_numeric(row_df[col], errors="ignore")
        row_df.dropna(how="all", subset=col, inplace=True)  # type: ignore

        if is_numeric_dtype(row_df[col]):
            row_df.rename(columns={col: "value_num"}, inplace=True)
            row_df["value_str"] = None
        else:
            row_df.rename(columns={col: "value_str"}, inplace=True)
            row_df["value_num"] = None
        row_df["value_num"] = row_df["value_num"].astype(float)
        row_df["question"] = col
        row_df["col_num"] = col_num

        row_df["sheet"] = sheet_name.replace(
            "...", "etc"
        )  # '...' not allowed in SharePoint file name
        row_df.insert(0, "year", self.year)
        return row_df[
            [
                "year",
                "Organization",
                "sheet",
                "row",
                "col_num",
                "question",
                "value_str",
                "value_num",
            ]
        ]

    def insert_df_into_table(
        self, df: pd.DataFrame, table_name: str, dbname: str, create_table: bool
    ):
        """
        Write a df to a DuckDB table
        """
        if create_table:
            logger.info(f"drop table {table_name}")
            self.eng.execute(f"""DROP TABLE IF EXISTS "{table_name}" """)
        # self.eng.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
        df.reset_index()
        index_label = None
        if (c := "Organization") in df.columns:
            df.set_index(c, inplace=True)
            index_label = c
        df.to_sql(
            table_name,
            self.eng,
            if_exists="append",
            schema="ref_data",
            index=True,
            index_label=index_label,
        )
        # self.eng.execute(f"INSERT INTO {table_name} SELECT * FROM df")

    def process_file(self):
        """
        Process the Excel file to  create the database

        We read all sheets names from the Excel file, then we read each sheet and process them.
        Sheet "CompanyData" is also stored  as-is
        For other sheets, we interate over each columns, and append the content to a table

        """
        logger.info(f"start processing {self.excel_file} to {self.dburl} ")
        sheets = getSheetsFromExcel(str(self.excel_file))
        sheet_count = 0
        for sheet_name in sheets:
            create_table = (sheet_count == 0) and REBUILD_TABLE
            logger.info(f"read  {sheet_name=}")
            df = pd.read_excel(self.excel_file, sheet_name=sheet_name)
            df.insert(0, "year", self.year)
            # in 2021 they has ta convenient sheet, "QuestionsList", but Pandas was unable to parse the link
            # so we needed to copy the values only in a new sheet, called QuestionsList_values
            if sheet_name in ["QuestionsList"]:
                pass
            elif sheet_name in ["QuestionsList_values"]:
                self.insert_df_into_table(
                    df, f"{sheet_name}_{self.year}", self.dburl, True
                )
            elif sheet_name in [
                "CompanyData",
                "Summary Data",
            ]:  # the name of the sheet as changed in 2020
                self.insert_df_into_table(
                    df, f"CompanyData_{self.year}", self.dburl, True
                )

            else:
                df.replace(
                    to_replace="Question not applicable", value=None, inplace=True
                )
                df.rename(
                    columns={"Row": "row", "RowName": "row_name"}, inplace=True
                )  # align to CDP2021 naming
                excluded_cols = {"Organization", "year", "row"} | COMMON_FIELDS
                columns = [c for c in df.columns if c not in excluded_cols]
                # delete rows when all values are "Question not applicable"
                if (c := "row_name") in df.columns:
                    df.dropna(how="all", subset=c, inplace=True)  # type: ignore

                col_count = 0
                for column in columns:
                    row_df = self.get_df_for_column(
                        df, sheet_name, str(column), col_count
                    )
                    self.insert_df_into_table(
                        row_df,
                        f"rows_{self.year}",
                        self.dburl,
                        create_table=create_table,
                    )
                    col_count += 1
                sheet_count += 1


app = typer.Typer()

DATALAKE = Path("/home/tcl/data4nz/")


@app.command()
def process_cdp_file(years: list[int]):
    """
    Main command
    """

    db_url = get_database_url_from_dbt_profile("organizations_make", target="dev_pg")
    for year in years:
        logger.info(f"Start processing {year}")
        runner = CdpExcelToSQL(year, DATALAKE, FILE_NAME[year], db_url)
        runner.process_file()
        logger.info("done")


if __name__ == "__main__":
    logger.remove()
    logger.add(
        sys.stderr, format="{level} - {time:HH:mm:ss}: {message} [{function}'{line})]"
    )
    if len(sys.argv) > 1:
        app()
    else:
        logger.warning("**** FOR TEST ******")
        # process_cdp_file(2022, DATALAKE)
