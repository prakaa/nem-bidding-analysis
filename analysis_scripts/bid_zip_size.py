from pathlib import Path
from typing import Dict, List

import matplotlib.pyplot as plt
import pandas as pd
from mms_monthly_cli.mms_monthly import get_table_names_and_sizes


def assemble_zipfile_size_data(start_year: int, end_year: int) -> Path:
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
    return out_path


def plot_zipfile_size_over_time(data_path: Path, start_year, end_year):
    df = pd.read_csv(data_path)
    df["Date"] = pd.to_datetime(
        df["month"].astype(str) + "/" + df["year"].astype(str), format="%m/%Y"
    )
    plt.style.use(Path("plots", "matplotlibrc.mplstyle"))
    fig, ax = plt.subplots(1, 1, figsize=(6, 4))
    df.plot(
        "Date",
        "BIDPEROFFER_size_GB",
        marker=".",
        markersize=8,
        linestyle="--",
        ax=ax,
        legend=False,
    )
    ax.set_xlabel("Date")
    ax.set_ylabel(
        "Size of monthly bidding data (BIDPEROFFER) zip file(s) (GB)"
    )
    ax.set_title("Monthly NEM Bidding Data Zipfile Size, 2012-2022")
    fig.savefig(
        Path(
            "plots", f"monthly_bidding_data_size_{start_year}_{end_year}.pdf"
        ),
        dpi=600,
    )


if __name__ == "__main__":
    out_path = assemble_zipfile_size_data(2012, 2023)
    plot_zipfile_size_over_time(out_path, 2012, 2023)
