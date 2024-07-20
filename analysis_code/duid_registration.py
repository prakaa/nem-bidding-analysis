from math import nan
from pathlib import Path

import pandas as pd

from .rebidding_analysis import get_gen_tech_mapping


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
    # only retain MASP and scheduled units
    tech_registration_scheduled = tech_registration[
        (tech_registration.Tech.isin(["MASP Pump", "DR/VPP", "Smelter", nan]))
        | (tech_registration["capacity_registered"] > 30)
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


def filter_by_date_and_tech(
    gen_tech_reg: pd.DataFrame, year: int, month: int, tech: str
) -> pd.DataFrame:
    """
    Find DUIDs of particular technology type that are operating in the given month
    based on when data was first seen and last seen for a given DUID.

    Includes any DUIDs that may have been retired during the month
    """
    assert tech in gen_tech_reg.Tech.unique(), f"{tech} not in gen_tech_reg"
    if month == 12:
        next_month = "01"
        end_filter_year = year + 1
    else:
        next_month = str(month + 1).rjust(2, "0")
        end_filter_year = year
    end_filter = f"{end_filter_year}-{next_month}-01"
    filtered = gen_tech_reg[
        (gen_tech_reg.Tech == tech)
        # ensure data first seen before end of month
        & (gen_tech_reg.data_first_seen < end_filter)
        # ensure data last seen before end of month
        & (gen_tech_reg.data_last_seen > end_filter)
    ]
    return filtered
