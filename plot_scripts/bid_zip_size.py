from datetime import datetime
from pathlib import Path
from typing import Dict, List

import matplotlib.pyplot as plt
import pandas as pd
from mms_monthly_cli.mms_monthly import get_table_names_and_sizes


def assemble_zipfile_size_data(start_year: int, end_year: int) -> None:
    data: Dict[str, List] = {}
    data["year"] = []
    data["month"] = []
    data["BIDPEROFFER_size_GB"] = []
    for year in range(start_year, end_year):
        for month in range(1, 13):
            print(f"Scraping {year} {month}")
            all_table_sizes = get_table_names_and_sizes(year, month, "DATA")
            bidperoffer = [
                tbname for tbname in all_table_sizes if "BIDPEROFFER" in tbname
            ]
            size = 0.0
            for fn in bidperoffer:
                size += all_table_sizes[fn]
            data["year"].append(year)
            data["month"].append(month)
            size /= 10**9
            data["BIDPEROFFER_size_GB"].append(size)
    df = pd.DataFrame(data)
    if not (data_out_path := Path("data", "processed")).exists():
        data_out_path.mkdir()
    out_path = data_out_path / Path(
        f"bidperoffer_monthly_zip_size_{start_year}_{end_year-1}.csv"
    )
    df.to_csv(str(out_path), index=False)
    return None


def plot_zipfile_size_over_time(data_path: Path, start_year, end_year):
    df = pd.read_csv(data_path)
    df["Date"] = pd.to_datetime(
        df["month"].astype(str) + "/" + df["year"].astype(str), format="%m/%Y"
    )
    plt.style.use(Path("plot_scripts", "matplotlibrc.mplstyle"))
    fig, ax = plt.subplots(1, 1, figsize=(6, 4))
    df.plot(
        "Date",
        "BIDPEROFFER_size_GB",
        marker=".",
        markersize=8,
        linestyle="--",
        linewidth=0.9,
        ax=ax,
        label="Monthly zip file size",
        legend=False,
    )
    ax.set_xlabel("Date")
    ax.set_ylabel("File size (GB)")
    ax.axvline(
        datetime(2021, 10, 1),
        0.0,
        1.0,
        ls="--",
        lw=1.0,
        color="black",
        label="5MS commencement",
    )
    ax.set_title("Monthly NEM Bid & Offer Data Zipfile Size, 2012-2022")
    fig.legend(
        ncol=2,
        loc="lower center",
        bbox_to_anchor=(0.5, -0.1),
        frameon=False,
    )
    fig.savefig(
        Path(
            "plots", f"monthly_bidding_data_size_{start_year}_{end_year}.pdf"
        ),
        dpi=600,
    )


if __name__ == "__main__":
    start_year = 2012
    end_year = 2023
    if not (
        out_path := Path(
            "data",
            "processed",
            f"bidperoffer_monthly_zip_size_{start_year}_{end_year-1}.csv",
        )
    ).exists():
        assemble_zipfile_size_data(start_year, end_year)
    plot_zipfile_size_over_time(out_path, start_year, end_year)
