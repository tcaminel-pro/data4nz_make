"""
  Utilities for Pandas DataFrame

  Copyright (C) 2023 Eviden. All rights reserved
"""
import pandas as pd
from unidecode import unidecode


def rename_duplicated_index(df: pd.DataFrame, suffix="#") -> pd.DataFrame:
    """
    Rename duplicated index in dataframe by adding 'suffix" and a number

    Needed because there are ducplicate rows in some LCA databases
    copied from https://stackoverflow.com/a/45240940
    """

    appendents = (
        suffix + df.groupby(level=0).cumcount().astype(str).replace("0", "")
    ).replace(suffix, "")
    return df.set_index(df.index + appendents, verify_integrity=True)


def df_columns_to_sql(df: pd.DataFrame, table_name="") -> str:
    """
    return the columns of a dataframe as a SQL SELECT clause to be cut & pasted
    """
    r = []
    for c in df.columns:
        col_name = str(c)
        new_name = (
            col_name.strip(" .,-#")
            .replace(" - ", "_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "_")
            .replace(":", "_")
            .replace(":", "_")
            .replace("(", "")
            .replace(")", "")
            .replace("(", "")
            .replace("/", "_")
            .replace(".", "_")
            .replace("?", "_")
            .lower()
        )
        new_name = unidecode(new_name).replace("__", "_")
        r.append(f""""{col_name}" as {new_name}""")
    j = ",\n   ".join(r)
    return f"""select\n   {j} \nfrom {table_name} """
