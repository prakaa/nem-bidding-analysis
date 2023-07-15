from datetime import datetime
from pathlib import Path
from typing import Dict, List

import matplotlib.pyplot as plt
import pandas as pd


def _make_100percent_stacked_bar_chart(
    percent_df: pd.DataFrame, color_map: Dict[str, str]
):
    fig, ax = plt.subplots(1, 1, figsize=(8, 6))
    last_value = None
    for col in percent_df:
        year_series = percent_df[col].sort_values(ascending=False)
        offset = 0.0
        for index, value in year_series.items():
            if last_value is not None:
                ax.bar(
                    int(col),
                    value,
                    bottom=offset,
                    label=index,
                    color=color_map[index],
                )
                offset += value
            else:
                ax.bar(int(col), value, label=index, color=color_map[index])
                offset = value
            if value > 3:
                ax.text(
                    int(col),
                    (offset - value / 2),
                    f"{int(value)}%",
                    c="white",
                    ha="center",
                    va="center",
                    fontsize=9,
                )
            last_value = value
    return fig, ax


def plot_rebid_counts_same_month_across_years(
    output_path: Path, path_to_mappings: Path, month_str: str
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
    fig, ax = _make_100percent_stacked_bar_chart(percent_by_year, tech_colors)
    year_totals = by_year.sum(axis=1)
    for index, item in year_totals.items():
        ax.text(
            index,
            102,
            f"{int(item)}",
            horizontalalignment="center",
            fontsize=12,
        )
    (handles, labels) = ax.get_legend_handles_labels()
    patches = handles[0 : int(len(handles) / len(data))]
    labels = labels[0 : int(len(labels) / len(data))]
    sorted_labels, sorted_patches = zip(*sorted(zip(labels, patches)))
    ax.legend(
        sorted_patches,
        sorted_labels,
        bbox_to_anchor=(0.5, -0.25),
        loc="lower center",
        borderaxespad=0,
        frameon=False,
        ncol=4,
    )
    ax.set_ylabel(
        f"Percentage of all rebids within 5 minutes in {month_str} (%)"
    )
    ax.set_title(
        f"Share of all rebids within 5 minutes of every DI in {month_str}, 2013-2021",
        pad=25,
    )
    return fig, ax


if __name__ == "__main__":
    plt.style.use(Path("plot_scripts", "matplotlibrc.mplstyle"))
    month_str = "July"
    fig, ax = plot_rebid_counts_same_month_across_years(
        Path(
            "data",
            "processed",
        ),
        Path("data", "mappings"),
        month_str,
    )
    fig.savefig(
        Path(
            "plots", f"rebids_{month_str.lower()}_share_by_tech_2013_2021.pdf"
        )
    )
