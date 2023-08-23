from pathlib import Path

import pandas as pd
from mms_monthly_cli.mms_monthly import get_and_unzip_table_csv
from rebidding_analysis import get_gen_tech_mapping

data_loc = Path("data", "raw")
get_and_unzip_table_csv(2022, 1, "DATA", "DISPATCHABLEUNIT", data_loc)
dispatchable = pd.read_csv(
    Path(data_loc, "PUBLIC_DVD_DISPATCHABLEUNIT_202201010000.CSV"), header=1
)
dispatchable = dispatchable.iloc[:-1, :]
duid_tech_mapping = get_gen_tech_mapping(
    Path("data", "mappings"), Path("data", "duids")
)
tech_registration = pd.merge(
    dispatchable, duid_tech_mapping, how="left", on="DUID"
)
tech_registration.LASTCHANGED = pd.to_datetime(
    tech_registration.LASTCHANGED, format="%Y/%m/%d %H:%M:%S"
)
gen_tech_reg = tech_registration[["DUID", "Tech", "LASTCHANGED"]]
vre_2018_2021 = (
    gen_tech_reg[
        (
            (gen_tech_reg.LASTCHANGED >= "2018-01-01")
            & (gen_tech_reg.LASTCHANGED < "2022-01-01")
        )
        & ((gen_tech_reg.Tech == "PV") | (gen_tech_reg.Tech == "Wind"))
    ]
    .set_index("LASTCHANGED")
    .sort_index()
)

bess_2018_2021 = (
    gen_tech_reg[
        (
            (gen_tech_reg.LASTCHANGED >= "2018-01-01")
            & (gen_tech_reg.LASTCHANGED < "2022-01-01")
        )
        & (gen_tech_reg.Tech == "Battery")
    ]
    .set_index("LASTCHANGED")
    .sort_index()
)

wind_until_2021 = (
    gen_tech_reg[
        (gen_tech_reg.LASTCHANGED < "2022-01-01")
        & (gen_tech_reg.Tech == "Wind")
    ]
    .set_index("LASTCHANGED")
    .sort_index()
)

ocgt_until_2021 = (
    gen_tech_reg[
        (gen_tech_reg.LASTCHANGED < "2022-01-01")
        & (gen_tech_reg.Tech == "OCGT")
    ]
    .set_index("LASTCHANGED")
    .sort_index()
)

print(
    "Total number of VRE plants registered between start of "
    + f"2018 & end of 2021: {vre_2018_2021.DUID.count()}"
)

# Divide by two to account for each BESS having gen and load DUID
print(
    "Total number of BESS plants registered between start of "
    + f"2018 & end of 2021: {int(bess_2018_2021.DUID.count() / 2)}"
)

print(
    "Total number of wind farms at end of 2021: "
    + f"{int(wind_until_2021.DUID.count())}"
)

print(
    "Total number of OCGT at end of 2021: "
    + f"{int(ocgt_until_2021.DUID.count())}"
)
