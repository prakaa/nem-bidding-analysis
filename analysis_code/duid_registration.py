from math import nan
from pathlib import Path

import numpy as np
import pandas as pd
from mms_monthly_cli.mms_monthly import get_and_unzip_table_csv

from .rebidding_analysis import get_gen_tech_mapping


def _get_dispatchable_unit(raw_data_loc: Path) -> pd.DataFrame:
    get_and_unzip_table_csv(2022, 1, "DATA", "DISPATCHABLEUNIT", raw_data_loc)
    dispatchable = pd.read_csv(
        Path(
            raw_data_loc,
            "PUBLIC_DVD_DISPATCHABLEUNIT_202201010000.CSV",
        ),
        header=1,
    )
    dispatchable = dispatchable.iloc[:-1, :]
    dispatchable.LASTCHANGED = dispatchable.LASTCHANGED.str.cat(
        np.repeat("+1000", len(dispatchable))
    )
    dispatchable.LASTCHANGED = pd.to_datetime(
        dispatchable.LASTCHANGED, format="%Y/%m/%d %H:%M:%S%z"
    )
    return dispatchable.set_index("DUID")


def get_duid_cap_tech_status_mapping(
    mapping_loc: Path, duid_loc: Path, raw_data_loc: Path
) -> pd.DataFrame:
    """
    Use OpenNEM facilities data to get table with capacity, technology and date data
    for each DUID.
    1. Only retain MASP or scheduled gen/loads (i.e. >30MW)
    2. Where date_first_seen is not available, use LASTCHANGED
    """
    tech_registration = get_gen_tech_mapping(mapping_loc, duid_loc)
    tech_registration.data_first_seen = pd.to_datetime(
        tech_registration.data_first_seen
    )
    # fill capacity_registered with Reg Cap if the first col is nan
    tech_registration["capacity_registered"] = tech_registration[
        "capacity_registered"
    ].fillna(tech_registration["Reg Cap (MW)"])
    # get LASTCHANGED into the table to use as a date filter where data_first_seen
    # is not available
    tech_registration = tech_registration.set_index("DUID")
    dispatchable = _get_dispatchable_unit(raw_data_loc)
    tech_registration = tech_registration.combine_first(dispatchable)
    # only keep entries with a tech type
    tech_registration = tech_registration[~tech_registration.Tech.isna()]
    # fill data_first_seen with LASTCHANGED if first col is nan
    tech_registration["data_first_seen"] = tech_registration[
        "data_first_seen"
    ].fillna(tech_registration["LASTCHANGED"])
    tech_registration = tech_registration.reset_index()
    # only retain MASP, Battery (all scheduled minus KEPBG/L1) and scheduled units
    tech_registration_scheduled = tech_registration[
        (tech_registration.Tech.isin(["MASP Pump", "DR/VPP", "Smelter", nan]))
        | (
            (tech_registration.Tech == "Battery")
            & ~(tech_registration.DUID.str.contains("KEP"))
        )
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
    ]
    if tech == "Steam (Coal, Gas)" or tech == "Hydro":
        # ensure data last seen before end of month
        filtered = filtered[filtered.data_last_seen > end_filter]
    return filtered
