"""
Utilities to get information from dbt files:
 - database credential from the dbt profile 
 - manifest file

Copyright (C) 2023 Eviden. All rights reserved
"""

from typing import Any
from devtools import debug
from loguru import logger
from ruamel.yaml import YAML
from pathlib import Path
import os

DEFAULT_PROFILES_DIR = os.path.join(os.path.expanduser("~"), ".dbt")


DEFAULT_PROFILES_DIR = Path.home() / ".dbt"


def load_file_contents(path: str, strip: bool = True) -> str:
    with open(path, "rb") as handle:
        to_return = handle.read().decode("utf-8")
    if strip:
        to_return = to_return.strip()
    return to_return


def read_dbt_profile(profiles_dir: Path = DEFAULT_PROFILES_DIR) -> dict[str, Any]:
    """
    Read the dbt profile (which contains information on target db, such as credentials)
    """
    yaml = YAML(typ="safe")
    path = Path(profiles_dir, "profiles.yml")
    if path.exists:
        yaml_content = yaml.load(path)
        if not yaml_content:
            msg = f"The profiles.yml file at {path} is empty"
            raise Exception(msg)
        return yaml_content
    return {}


def read_dbt_model(path: Path) -> dict[str, Any]:
    """
    Read a dbt model
    """
    yaml = YAML(typ="safe")
    if path.exists:
        yaml_content = yaml.load(path)
        if not yaml_content:
            msg = f"The profiles.yml file at {path} is empty"
            raise Exception(msg)
        return yaml_content
    return {}


def get_database_url_from_dbt_profile(
    dbt_prj_name: str, profiles_dir: str = DEFAULT_PROFILES_DIR, target: str = "dev"
) -> str:
    """
    Get the target URL from the dbt profile in the home directory
    """
    dbt_profile = read_dbt_profile(profiles_dir)
    try :
        r = dbt_profile[dbt_prj_name]["outputs"][target]
    except KeyError as ex:
        logger.error(ex)
        debug(dbt_profile[dbt_prj_name])
        raise ex
    if r["type"] == "postgres":
        db_url = f"postgresql+pg8000://{r['user']}:{r['pass']}@{r['host']}:{r['port']}/{r['dbname']}"
    elif r["type"] == "duckdb":
        db_url = f"duckdb:///{r['path']}"
    else:
        raise ValueError(f"cant't find DB URL from DBT project {dbt_prj_name} ")
    return db_url


"""
def get_db_url_from_dbt(dbt_profiles_dir="~/.dbt", dbt_project_dir="../..") -> str:

    faldbt = FalDbt(profiles_dir=dbt_profiles_dir, project_dir=dbt_project_dir)
    r = faldbt._config.credentials
    return f"postgresql+pg8000://{r.user}:{r.password}@{r.host}:{r.port}/{r.database}"

"""
