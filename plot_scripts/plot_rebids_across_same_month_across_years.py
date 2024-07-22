from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

import matplotlib.pyplot as plt
import pandas as pd

from analysis_code.duid_registration import (
    filter_by_date_and_tech,
    get_duid_cap_tech_status_mapping,
)


def _make_100percent_stacked_bar_chart(
    percent_df: pd.DataFrame,
    month: int,
    color_map: Dict[str, str],
    path_to_mappings: Path,
    path_to_duids: Path,
    path_to_raw: Path,
):
    duid_tech_map = get_duid_cap_tech_status_mapping(
        path_to_mappings, path_to_duids, path_to_raw
    )
    fig, ax = plt.subplots(1, 1, figsize=(10, 6))
    last_value = None
    interval = timedelta(days=365)
    percent_df = percent_df.sort_values(2020, ascending=False)
    for col in percent_df:
        year = datetime.strptime(str(col), "%Y")
        offset = 0.0
        year_series = percent_df[col]
        for index, value in year_series.items():
            if last_value is not None:
                ax.bar(
                    year,
                    value,
                    bottom=offset,
                    label=index,
                    color=color_map[index],
                    width=interval,
                )
                offset += value
            else:
                ax.bar(
                    year,
                    value,
                    label=index,
                    color=color_map[index],
                    width=interval,
                )
                offset = value
            if value > 3:
                duids = filter_by_date_and_tech(
                    duid_tech_map, year.year, month, index
                )
                # because batteries have a gen and load DUID
                if index == "Battery":
                    duid_counts = len(duids) / 2
                # four smelters in data, but Point Henry has been closed since 2014
                elif index == "Smelter":
                    duid_counts = 1
                else:
                    duid_counts = len(duids)

                ax.text(
                    year,
                    (offset - value / 2),
                    f"{int(value)}% ({int(duid_counts)})",
                    c="white",
                    ha="center",
                    va="center",
                    fontsize=9,
                )
            last_value = value
    return fig, ax


def plot_rebid_counts_same_month_across_years(
    output_path: Path,
    path_to_mappings: Path,
    path_to_duids: Path,
    path_to_raws: Path,
    month_str: str,
):
    month = datetime.strptime(month_str, "%B").month
    data: List[pd.DataFrame] = []
    for file in output_path.glob(f"rebid_counts_{month}*.parquet"):
        df_month = pd.read_parquet(file)
        data.append(df_month)
    df = pd.concat(data, axis=0)
    df["Year"] = df.index.year
    by_year = df.groupby("Year").sum()
    percent_by_year = (by_year.div(by_year.sum(axis=1), axis=0) * 100).T
    tech_colors = pd.read_json(
        path_to_mappings / Path("color_techtype_mapping.json"), typ="series"
    )
    fig, ax = _make_100percent_stacked_bar_chart(
        percent_by_year,
        month,
        tech_colors,
        path_to_mappings,
        path_to_duids,
        path_to_raws,
    )
    year_totals = by_year.sum(axis=1)
    for index, item in year_totals.items():
        ax.text(
            datetime.strptime(str(index), "%Y"),
            102,
            f"{format(int(item), ',')}",
            horizontalalignment="center",
            fontsize=12,
        )
    (handles, labels) = ax.get_legend_handles_labels()
    patches = handles[0 : int(len(handles) / len(data))]
    labels = labels[0 : int(len(labels) / len(data))]
    ax.legend(
        reversed(patches),
        reversed(labels),
        bbox_to_anchor=(1.12, 0.25),
        loc="lower center",
        borderaxespad=0,
        frameon=False,
        ncol=1,
    )
    ax.set_ylabel("Percentage (%)")
    ax.set_title(
        ("Rebids by Technology Type " + f"in {month_str} — 2013-2021"),
        pad=25,
    )
    years = [y for y in range(2013, 2022, 1)]
    ax.xaxis.set_ticks(
        [datetime.strptime(str(y), "%Y") for y in years],
        [str(y) for y in years],
    )
    return fig, ax


if __name__ == "__main__":
    plt.style.use(Path("plot_scripts", "matplotlibrc.mplstyle"))
    month_str = "June"
    fig, ax = plot_rebid_counts_same_month_across_years(
        Path(
            "data",
            "processed",
        ),
        Path("data", "mappings"),
        Path("data", "duids"),
        Path("data", "raw"),
        month_str,
    )
    fig.savefig(
        Path(
            "plots",
            (f"rebids_{month_str.lower()}_" + "share_by_tech_2013_2021.pdf"),
        )
    )
