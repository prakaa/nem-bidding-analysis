# %%
from datetime import datetime, timedelta
from pathlib import Path

# import matplotlib.pyplot as plt
import pandas as pd
import polars as pl


# %%
def get_gen_tech_color_mapping(path_to_mappings: Path) -> pd.DataFrame:
    gen_loads = pd.read_csv(path_to_mappings / Path("cleaned_gen_loads.csv"))
    non_gen_loads = pd.read_csv(
        path_to_mappings / Path("non_genloads_duid_providers_with_techs.csv")
    )
    simple_techs = pd.read_json(
        path_to_mappings / Path("techtype_simple_mapping.json"), typ="series"
    )
    tech_colors = pd.read_json(
        path_to_mappings / Path("color_techtype_mapping.json"), typ="series"
    )
    tech_colors.name = "Tech"
    combined = pd.concat([gen_loads, non_gen_loads], axis=0)
    combined = combined.replace(simple_techs)
    combined.rename(
        columns={"Technology Type - Descriptor": "Tech"}, inplace=True
    )
    combined["Colour"] = combined["Tech"].replace(tech_colors)
    keep_cols = [
        "Participant",
        "Region",
        "DUID",
        "Station Name",
        "Tech",
        "Reg Cap (MW)",
        "Colour",
    ]
    combined = combined[keep_cols]
    return combined


def get_bid_data_for_periods(
    partitioned_data_path: Path,
    day_col: str,
    day: datetime,
    period_start: int,
    period_end: int,
) -> pd.DataFrame:
    """
    NEM day starts at 4AM, hence add 4 hours in addition to PERIODID
    """
    q = pl.scan_parquet(
        partitioned_data_path
        / Path(day_col)
        / Path(day.strftime("%Y%m%d") + "*.parquet")
    ).filter(
        (
            pl.col("PERIODID").is_between(
                period_start, period_end, closed="both"
            )
        )
    )
    df = q.collect()
    df = df.to_pandas()
    df[day_col + "TIME"] = (
        df[day_col]
        + pd.Timedelta(minutes=5) * df.PERIODID
        + pd.Timedelta(hours=4)
    )
    offer_col = [col for col in df.columns if "OFFERDATE" in col].pop()
    df["REBIDAHEADTIME"] = df[day_col + "TIME"] - df[offer_col]
    return df


def get_all_rebids_less_than_ahead_time(
    df: pd.DataFrame, ahead_time: timedelta
) -> pd.DataFrame:
    """
    There appears to be a large number of bids submitted after the dispatch interval
    of interest. Hence the lower bound of `> pd.Timedelta(minutes=0)`.
    """
    filtered = df[df["REBIDAHEADTIME"] > pd.Timedelta(minutes=0)]
    filtered = filtered[filtered["REBIDAHEADTIME"] <= ahead_time]
    return filtered


def count_rebids_by_tech(
    df: pd.DataFrame, ahead_time: timedelta, path_to_mappings: Path
):
    mapping = get_gen_tech_color_mapping(path_to_mappings)
    filtered = get_all_rebids_less_than_ahead_time(df, ahead_time)
    merged = pd.merge(
        filtered,
        mapping,
        on="DUID",
        how="left",
    )
    merged["Tech"].fillna("Unknown", inplace=True)
    return merged


# x = count_rebids_by_tech(test, timedelta(minutes=10), Path("data", "mappings"))
#
# # %%
# rebid_cols = [col for col in df.columns if "TIME" in col] + [
#     "DUID",
#     "Technology Type - Descriptor",
# ]
# rebid = df[rebid_cols].drop_duplicates()
# # %%
# x = (
#     rebid.groupby("Technology Type - Descriptor")["DUID"]
#     .count()
#     .rename("REBIDS")
# )
#
# plt.style.use("../matplotlibrc.mplstyle")
# plt.pie(x, labels=x.index)
# plt.legend(loc="lower center")
#
# # %%
# y = rebid[rebid["REBIDAHEADTIME"] < pd.Timedelta(hours=1)]
# y = y.groupby("Technology Type - Descriptor")["DUID"].count().rename("REBIDS")
# plt.style.use("../matplotlibrc.mplstyle")
# plt.pie(y, labels=y.index)
# plt.legend(loc="lower center")
#
# # %%
# z = rebid[rebid["REBIDAHEADTIME"] < pd.Timedelta(minutes=5)]
# z = z.groupby("Technology Type - Descriptor")["DUID"].count().rename("REBIDS")
# plt.style.use("../matplotlibrc.mplstyle")
# plt.pie(z, labels=z.index)
# plt.legend(loc="lower center")
#
# # %%
#
# # %%
# z = rebid[rebid["Technology Type - Descriptor"] == "Unknown"]
# z = z.groupby("DUID")["DUID"].count().rename("REBIDS")
# plt.style.use("../matplotlibrc.mplstyle")
# plt.pie(z, labels=z.index)
# plt.legend(loc="lower center")
# # %%
#
