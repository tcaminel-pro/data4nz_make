import contextlib
import re
from datetime import datetime
from re import Match
from typing import Callable
from typing import Dict
from typing import Iterator
from typing import List
from typing import Tuple
from typing import Union
from collections import defaultdict


import nltk
import pandas as pd
from data_model import AlignmentData
from data_model import Pattern
from data_model import Scope
from nltk import tokenize

import os, sys
import fal

## quick and very dirty   @TODO : find a better solution
dir = "/mnt/c/GitAtos/emission_factors_make/python"
if not dir in sys.path:
    sys.path.append(dir)
from dbt_utils import get_database_url

# nltk.download('punkt')

FIRST_PATTERN = (
    r"reduce (?P<scope>.*?) \be\S* (?P<coverage1>.*?) (?P<reduction_ambition1>\d.*?%)"
    r" .*? .*?(?P<end_year1>\d{4}).*? .*?(?P<coverage2>.*?) "
    r"(?P<reduction_ambition2>\d.*?%) .*? .*?(?P<end_year2>\d{4}).*? "
    r".*?(?P<coverage3>.*?) (?P<reduction_ambition3>\d.*?%) .*? "
    r".*?(?P<end_year3>\d{4}).*? (?P<base_year>\d{4})"
)
SECOND_PATTERN = (
    r"reduce (?P<scope1>.*?\d) \be\S* (?P<reduction_ambition1>.*?\d+\s?%) "
    r".*?(?P<end_year1>\d{4}) .*?(?P<base_year1>\d{4}) .*? reduce "
    r"(?P<scope2>.*?\d) \be\S* (?P<reduction_ambition2>.*?\d+\s?%) "
    r".*?(?P<end_year2>\d{4}) .*?(?P<base_year2>\d{4})"
)
THIRD_PATTERN = (
    r"reduce (?P<scope>.*?) \be\S* (?P<reduction_ambition>.*?\d+.?\s?\d+%)"
    r"(?P<metric>.*?) (?P<end_year>\d{4}).*? (?P<base_year>\d{4})"
)
FOURTH_PATTERN = (
    r"reduce (?P<scope>.*?) \be\S* (?P<coverage>.*?) "
    r"(?P<reduction_ambition1>\d+).*? (?P<end_year1>\d{4}) and "
    r"(?P<reduction_ambition2>\d+).*? (?P<end_year2>\d{4}).*? (?P<base_year>\d{4})"
)
FIFTH_PATTERN = r"reduce (?P<scope>.*?) (?P<reduction_ambition>\d+%.*?) \d{4}.*? \d{4}"
SIXTH_PATTERN = (
    r"reduce (?P<scope>.*?) \be\S* (?P<reduction_ambition>\d+.*?)%"
    r" .*? timeframe (?P<coverage> .*)"
)
SEVENTH_PATTERN = (
    r"(reduce|reducing) (?P<scope>.*?) \be\S* "
    r"(?P<coverage>.*?)(?P<reduction_ambition>"
    r"\d+\.?\d+?\s?% .*?) (timefram|time frame|timeframe|period|time-period)"
)
EIGHTH_PATTERN = (
    r"commits to (?:reduce|reducing)(?P<scope>.*?) \be\S* .*? "
    r"(?P<reduction_ambition>\d+|zero)(?P<metric>.*?) (?P<end_year>\d{4})"
)
NINTH_PATTERN = r"maintain (?P<scope>.*?) \be\S* (?P<coverage>.*?) (?P<end_year>\d{4})"
TENTH_PATTERN = (
    r"commits to reduce (?P<coverage>.*?) (?P<reduction_ambition>\d+) .*? "
    r"(?P<end_year>\d{4})"
)

SCOPE_CONVERTER: Dict[str, Scope] = {
    "123": Scope.S123,
    "12": Scope.S12,
    "1": Scope.S1,
    "2": Scope.S2,
    "3": Scope.S3,
}

PATTERN_REGEXES: Dict[Pattern, str] = {
    Pattern.First: FIRST_PATTERN,
    Pattern.Second: SECOND_PATTERN,
    Pattern.Third: THIRD_PATTERN,
    Pattern.Fourth: FOURTH_PATTERN,
    Pattern.Fifth: FIFTH_PATTERN,
    Pattern.Sixth: SIXTH_PATTERN,
    Pattern.Seventh: SEVENTH_PATTERN,
    Pattern.Eighth: EIGHTH_PATTERN,
    Pattern.Ninth: NINTH_PATTERN,
    Pattern.Tenth: TENTH_PATTERN,
}

BasicType = Union[int, float, str, bool]
UnionType = Union[BasicType, Tuple[BasicType, BasicType]]


def group_by_value(list_to_group: List[UnionType]) -> Dict[UnionType, List[int]]:
    indices: Dict[UnionType, List[int]] = defaultdict(list)
    for i in range(len(list_to_group)):
        indices[list_to_group[i]].append(i)
    return indices


def convert_data_to_df(data: List[AlignmentData]) -> pd.DataFrame:
    """
    Converts the object into a dataframe
    """
    df = pd.DataFrame.from_dict(
        {
            "company_name": [elt.company_name for elt in data],
            "target_type": [elt.target_type for elt in data],
            "scope": [elt.scope for elt in data],
            "intensity_metric": [elt.metric for elt in data],
            "coverage": [elt.coverage for elt in data],
            "reduction_ambition": [elt.reduction_ambition for elt in data],
            "base_year": [elt.base_year for elt in data],
            "end_year": [elt.end_year for elt in data],
            "start_year": [elt.start_year for elt in data],
            "target": [elt.target for elt in data],
            "set": [elt.set for elt in data],
        }
    )

    for key, row in df.iterrows():
        if row.base_year == 0:
            values = group_by_value(df["company_name"])[row.company_name]
            df.loc[key, "base_year"] = df["base_year"][min(values)]
            df.loc[key, "end_year"] = df["end_year"][max(values)]

    return df


def clean_companies(df: pd.DataFrame) -> pd.DataFrame:
    """
    Loop through the different targets
    """

    all_data: List[AlignmentData] = []
    for _, row in df.iterrows():
        if row["near_term_target_status"] != "Committed":
            all_data.extend(
                list(
                    clean_company(
                        row["target"], row["company_name"], get_year(row["date"])
                    )
                )
            )
        else:
            all_data.append(AlignmentData(company_name=row["company_name"], set=False))

    return convert_data_to_df(all_data)


def get_year(raw_date: str) -> int:
    """
    Convert the passed raw_date to a year. 0 default value
    """
    if isinstance(raw_date, str):
        return datetime.strptime(raw_date, "%d/%m/%Y").year
    return 0


def clean_company(
    target: str, company_name: str, start_year: int
) -> Iterator[AlignmentData]:
    """
    Loop through the sentences
    """

    for sentence in tokenize.sent_tokenize(target):
        if "increase annual sourcing of renewable electricity" in sentence:
            continue
        if get_alignment_data(re.sub(r"GHG |FY |by |from ", "", sentence)):
            for data in get_alignment_data(re.sub(r"GHG |FY |by |from ", "", sentence)):
                data.company_name = company_name
                data.start_year = start_year
                data.target = sentence
                yield data


def get_alignment_data(sentence: str) -> List[AlignmentData]:
    """
    Loop through the patterns
    """
    for pattern in Pattern:
        if re.search(PATTERN_REGEXES[pattern], sentence) is not None:
            match = re.search(PATTERN_REGEXES[pattern], sentence)
            return list(PATTERN_METHODS[pattern](match))
    return []


def get_first_pattern(match: Match) -> Iterator[AlignmentData]:
    """
    Extracts the relevant info from the first pattern
    """
    scope = get_scope(match.group("scope"))
    coverages = [match.group(x) for x in ["coverage1", "coverage2", "coverage3"]]
    reduction_ambitions = [
        int(match.group(x).strip("%")) / 100
        for x in ["reduction_ambition1", "reduction_ambition2", "reduction_ambition3"]
    ]
    end_years = [match.group(x) for x in ["end_year1", "end_year2", "end_year3"]]
    base_year = match.group("base_year")

    for coverage, reduction_ambition, end_year in zip(
        coverages, reduction_ambitions, end_years
    ):
        yield AlignmentData(
            scope=scope,
            coverage=coverage,
            reduction_ambition=reduction_ambition,
            end_year=end_year,
            base_year=base_year,
        )


def get_second_pattern(match: Match) -> Iterator[AlignmentData]:
    """
    Extracts the relevant info from the second pattern
    """
    scopes = [get_scope(match.group(x)) for x in ["scope1", "scope2"]]
    reducs = [
        int(re.findall(r"\d+", match.group(x))[0]) / 100
        for x in ["reduction_ambition1", "reduction_ambition2"]
    ]
    end_years = [match.group(x) for x in ["end_year1", "end_year2"]]
    base_years = [match.group(x) for x in ["base_year1", "base_year2"]]
    for scope, reduc_ambition, end_year, base_year in zip(
        scopes, reducs, end_years, base_years
    ):
        yield AlignmentData(
            scope=scope,
            reduction_ambition=reduc_ambition,
            end_year=end_year,
            base_year=base_year,
        )


def get_third_pattern(match: Match) -> Iterator[AlignmentData]:
    """
    Extracts the relevant info from the third pattern
    """

    scope = get_scope(match.group("scope"))
    reduction_ambition = (
        int(re.findall(r"\d+", match.group("reduction_ambition"))[0]) / 100
    )
    coverage = (
        m[1] if (m := re.search(r"(.*?)\d", match.group("reduction_ambition"))) else ""
    )
    metric = match.group("metric") if "per" in match.group("metric") else ""
    target_type = _get_target_type(match, metric)
    base_year, end_year = _get_base_end_years(re.findall(r"\d{4}", match[0]))
    yield AlignmentData(
        target_type=target_type,
        scope=scope,
        reduction_ambition=reduction_ambition,
        coverage=coverage,
        base_year=base_year,
        end_year=end_year,
        metric=metric,
    )


def get_fourth_pattern(match: Match) -> Iterator[AlignmentData]:
    """
    Extracts the relevant info from the 4th pattern
    """
    target_type = "absolute" if re.search("absolute", match.group("scope")) else ""
    scope = get_scope(match.group("scope"))
    coverage = match.group("coverage")
    base_year = int(match.group("base_year"))
    reduc_1 = int(match.group("reduction_ambition1")) / 100
    end_year_1 = int(match.group("end_year1"))
    reduc_2 = int(match.group("reduction_ambition2")) / 100
    end_year_2 = int(match.group("end_year2"))
    for reduction_ambition, end_year in zip(
        [reduc_1, reduc_2], [end_year_1, end_year_2]
    ):
        yield AlignmentData(
            target_type=target_type,
            scope=scope,
            coverage=coverage,
            reduction_ambition=reduction_ambition,
            base_year=base_year,
            end_year=end_year,
        )


def get_fifth_pattern(match: Match) -> Iterator[AlignmentData]:
    """
    Extracts the relevant info from the 5th pattern
    """
    scope = get_scope(match.group("scope"))
    coverage = m[1] if (m := re.search(r"\be\S* (.*)", match.group("scope"))) else ""
    reduction_ambition = (
        int(re.findall(r"\d+", match.group("reduction_ambition"))[0]) / 100
    )
    metric = (
        m[1] if (m := re.search(r"(per .*)", match.group("reduction_ambition"))) else ""
    )
    target_type = _get_target_type(match, metric)
    base_year, end_year = _get_base_end_years(re.findall(r"\d{4}", match[0]))

    yield AlignmentData(
        target_type=target_type,
        scope=scope,
        coverage=coverage,
        end_year=end_year,
        reduction_ambition=reduction_ambition,
        metric=metric,
        base_year=base_year,
    )


def get_sixth_pattern(match: Match) -> Iterator[AlignmentData]:
    """
    Extracts the relevant info from the 6th pattern
    """
    scope = get_scope(match.group("scope"))
    target_type = "Absolute" if re.search("absolute", match.group("scope")) else ""
    reduction_ambition = (
        int(re.findall(r"\d+", match.group("reduction_ambition"))[0]) / 100
    )
    coverage = match.group("coverage")
    yield AlignmentData(
        scope=scope,
        target_type=target_type,
        reduction_ambition=reduction_ambition,
        coverage=coverage,
    )


def get_seventh_pattern(match: Match) -> Iterator[AlignmentData]:
    """
    Extracts the relevant info from the 7th pattern
    """

    scope = get_scope(match.group("scope"))
    reduction_ambition = (
        int(re.findall(r"\d+", match.group("reduction_ambition"))[0]) / 100
    )
    metric = (
        m[1] if (m := re.search(r"(per .*)", match.group("reduction_ambition"))) else ""
    )
    target_type = _get_target_type(match, metric)
    coverage = match.group("coverage")

    yield AlignmentData(
        target_type=target_type,
        scope=scope,
        reduction_ambition=reduction_ambition,
        metric=metric,
        coverage=coverage,
    )


def get_eighth_pattern(match: Match) -> Iterator[AlignmentData]:
    """
    Extracts the relevant info from the 8th pattern
    """

    scope = get_scope(match.group("scope"))
    reduction_ambition = (
        0
        if "zero" in match.group("reduction_ambition")
        else int(match.group("reduction_ambition")) / 100
    )
    metric = m[1] if (m := re.search(r"(per .*)", match.group("metric"))) else ""
    target_type = _get_target_type(match, metric)
    end_year = int(match.group("end_year"))

    yield AlignmentData(
        target_type=target_type,
        scope=scope,
        reduction_ambition=reduction_ambition,
        metric=metric,
        end_year=end_year,
    )


def get_ninth_pattern(match: Match) -> Iterator[AlignmentData]:
    """
    Extracts the relevant info from the 9th pattern
    """

    target_type = "absolute" if re.search("absolute", match.group("scope")) else ""
    scope = get_scope(match.group("scope"))
    reduction = (
        0 if "zero" in match.group("scope") or "zero" in match.group("coverage") else ""
    )
    coverage = match.group("coverage")
    end_year = int(match.group("end_year"))
    yield AlignmentData(
        target_type=target_type,
        scope=scope,
        reduction_ambition=reduction,
        coverage=coverage,
        end_year=end_year,
    )


def get_tenth_pattern(match: Match) -> Iterator[AlignmentData]:
    """
    Extracts the relevant info from the 10th pattern
    """

    coverage = match.group("coverage")
    reduction_ambition = int(match.group("reduction_ambition")) / 100
    end_year = int(match.group("end_year"))

    yield AlignmentData(
        coverage=coverage, reduction_ambition=reduction_ambition, end_year=end_year
    )


def _get_target_type(match: Match, metric: str) -> str:
    """
    Retrieve the target type from the passed match and metric
    """
    if re.search("absolute", match.group("scope")):
        return "Absolute"
    if metric:
        return "Intensity"
    return ""


def _get_base_end_years(years: List[str]) -> Tuple[int, int]:
    """
    Convert passed year to base and end years
    """
    converted_years = [int(year) for year in years]
    return min(converted_years, default=0), max(converted_years, default=0)


def get_scope(scopes: str) -> str:
    """
    Convert passed scopes to a Scope and then take its string value
    """
    if groups := re.findall(r"\d", scopes):
        with contextlib.suppress(KeyError):
            return str(SCOPE_CONVERTER["".join(sorted(set(groups)))].value)
    return ""


PATTERN_METHODS: Dict[Pattern, Callable] = {
    Pattern.First: get_first_pattern,
    Pattern.Second: get_second_pattern,
    Pattern.Third: get_third_pattern,
    Pattern.Fourth: get_fourth_pattern,
    Pattern.Fifth: get_fifth_pattern,
    Pattern.Sixth: get_sixth_pattern,
    Pattern.Seventh: get_seventh_pattern,
    Pattern.Eighth: get_eighth_pattern,
    Pattern.Ninth: get_ninth_pattern,
    Pattern.Tenth: get_tenth_pattern,
}


def main():
    from fal import FalDbt

    print("Start SBT Target processing")
    prj_dir = os.getcwd().rpartition("python")[0]  # best guess..
    faldbt = FalDbt(profiles_dir="~/.dbt", project_dir=prj_dir)

    input_df = faldbt.ref("stg_sbt_companies")
    input_df = input_df.head(50)  # for quicker tests....
    processed_df = clean_companies(input_df)
    # in next fal version (1.3), that should work well :
    # faldbt.write_to_model(processed_df,'sbt_target_processed')
    # meantime, here a hack:
    r = faldbt._config.credentials
    db_url = f"postgresql+pg8000://{r.user}:{r.password}@{r.host}:{r.port}/{r.database}"
    processed_df.to_sql("sbt_target_processed", db_url, if_exists="replace", index=None)
    #


main()
