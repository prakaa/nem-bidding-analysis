from pathlib import Path

import pandas as pd
from rebidding_analysis import get_gen_tech_mapping


def get_duid_cap_tech_status_mapping(
    mapping_loc: Path, duid_loc: Path
) -> pd.DataFrame:
    """
    Merge DISPATCHABLEUNIT with Generator and Loads reg and opennem facilities data
    to get a table with data_first_seen, LASTCHANGED, capacity and status data for
    each DUID
    """
    tech_registration = get_gen_tech_mapping(mapping_loc, duid_loc)
    tech_registration.data_first_seen = pd.to_datetime(
        (tech_registration.data_first_seen)
    )
    # only retain scheduled units
    tech_registration_scheduled = tech_registration[
        tech_registration["capacity_registered"] > 30
    ]
    gen_tech_reg = tech_registration_scheduled[
        [
            "DUID",
            "Tech",
            "data_first_seen",
            "data_last_seen",
            "status",
        ]
    ]
    return gen_tech_reg


def filter_by_year_and_tech(
    gen_tech_reg: pd.DataFrame, year: int, tech: str
) -> pd.DataFrame:
    """
    Find DUIDs of particular technology type that are operating in the given year
    based on when data was first seen and last seen for a given DUID.

    Includes any DUIDs that may have been retired mid-year
    """
    assert tech in gen_tech_reg.Tech.unique()
    # start of the next year
    year_filter = f"{year}-01-01"
    next_year_filter = f"{year+1}-01-01"
    filtered = gen_tech_reg[
        (gen_tech_reg.Tech == tech)
        # ensure data first seen before the year
        & (gen_tech_reg.data_first_seen < next_year_filter)
        # ensure data seen after the year
        & (gen_tech_reg.data_last_seen >= year_filter)
    ]
    return filtered


gen_tech_reg = get_duid_cap_tech_status_mapping(
    Path("data", "mappings"), Path("data", "duids")
)
