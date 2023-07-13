# Python script (executable via CLI) to creae parquet partitions
# for large AEMO data CSVs. Assumes first line is table header and that
# only one table type is in the file
#
# Copyright (C) 2023 Abhijith Prakash
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import argparse
import logging
from glob import glob
from pathlib import Path

import numpy as np
import pandas as pd
from tqdm import tqdm

"""Some columns that would otherwise be integer are cast to float to handle NaN
"""
bidperoffer_dtypes = {
    "I": str,
    "BIDS": str,
    "BIDOFFERPERIOD": str,
    "1": np.float32,
    "DUID": str,
    "BIDTYPE": str,
    "PERIODID": np.float32,
    "MAXAVAIL": np.float32,
    "FIXEDLOAD": np.float32,
    "RAMPUPRATE": np.float64,
    "RAMPDOWNRATE": np.float64,
    "ENABLEMENTMIN": np.float32,
    "ENABLEMENTMAX": np.float32,
    "LOWBREAKPOINT": np.float32,
    "HIGHBREAKPOINT": np.float32,
    "BANDAVAIL1": np.float32,
    "BANDAVAIL2": np.float32,
    "BANDAVAIL3": np.float32,
    "BANDAVAIL4": np.float32,
    "BANDAVAIL5": np.float32,
    "BANDAVAIL6": np.float32,
    "BANDAVAIL7": np.float32,
    "BANDAVAIL8": np.float32,
    "BANDAVAIL9": np.float32,
    "BANDAVAIL10": np.float32,
    "PASAAVAILABILITY": np.float64,
}

dt_format = "%Y/%m/%d %H:%M:%S"
strf_format = "%Y%m%d"


def arg_parser():
    description = (
        "Chunk large monthly AEMO data table CSVs into parquet partitions. "
        + "Assumes that the table header is in the 2nd row"
    )
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "-file", type=str, required=True, help=("File to process. Must be CSV")
    )
    parser.add_argument(
        "-output_dir",
        type=str,
        required=True,
        help=(
            "Directory to write parquet chunks to. "
            + "Will be created if it does not exist"
        ),
    )
    parser.add_argument(
        "-chunksize",
        type=int,
        default=10**6,
        help=("Size of each DataFrame chunk (# of lines). Default 10^6"),
    )
    args = parser.parse_args()
    return args


def get_columns(file_path: Path) -> pd.Index:
    col_df = pd.read_csv(file_path, header=1, nrows=0)
    return col_df.columns


def estimate_size_of_lines(file_path: Path, columns=pd.Index) -> float:
    sample_size = 1000
    sample = pd.read_csv(file_path, skiprows=2, nrows=sample_size, header=None)
    sample.columns = columns
    total_size = sample.memory_usage().sum()
    size_per_line = total_size / len(sample)
    return size_per_line


def write_chunks_by_trading_date(
    chunk: pd.DataFrame, output_dir: Path
) -> None:
    unique_trading_dates = chunk["TRADINGDATE"].unique().tolist()
    for trading_date in unique_trading_dates:
        td_chunk = chunk.loc[chunk["TRADINGDATE"] == trading_date, :]
        str_td = trading_date.strftime(strf_format)
        base_file_name = Path(output_dir, str_td + "-chunk-")
        if not (
            sorted_written_chunks := sorted(
                glob(str(base_file_name) + "*.parquet")
            )
        ):
            last_chunk_number = 0
        else:
            last_chunk_number = int(Path(sorted_written_chunks[-1]).stem[-3:])
        chunk_number = last_chunk_number + 1
        filename = Path(
            str(base_file_name) + str(chunk_number).rjust(3, "0") + ".parquet"
        )
        td_chunk.to_parquet(filename, engine="pyarrow")
    return None


def chunk_file(file_path: Path, output_dir: Path, chunksize: int) -> None:
    if not file_path.suffix.lower() == ".csv":
        logging.error("File is not a CSV")
        exit()
    cols = get_columns(file_path)
    size_per_line = estimate_size_of_lines(file_path, cols)
    file_size = file_path.stat().st_size
    if "BIDPEROFFER" in file_path.stem:
        logging.info("Recognised BIDPEROFFER CSV")
        dtypes = bidperoffer_dtypes
    else:
        dtypes = None
    previous_chunk = None
    with pd.read_csv(
        file_path,
        chunksize=chunksize,
        skiprows=2,
        names=cols,
        dtype=dtypes,
        parse_dates=["TRADINGDATE", "OFFERDATETIME"],
        date_format=dt_format,
    ) as reader:
        with tqdm(
            total=file_size, desc="Progress estimate based on file size"
        ) as pbar:
            for chunk in reader:
                if previous_chunk is not None:
                    write_chunks_by_trading_date(previous_chunk, output_dir)
                previous_chunk = chunk
                # See here for comparison of pandas DataFrame size vs CSV size:
                # https://stackoverflow.com/questions/18089667/how-to-estimate-how-much-memory-a-pandas-dataframe-will-need#32970117
                pbar.update((size_per_line * chunksize) / 2)
            write_chunks_by_trading_date(
                previous_chunk.iloc[:-1], output_dir  # type: ignore
            )


def main():
    logging.basicConfig(
        format="\n%(levelname)s:%(message)s", level=logging.INFO
    )
    args = arg_parser()
    f = Path(args.file)
    output_dir = Path(args.output_dir)
    if not output_dir.exists():
        output_dir.mkdir(parents=True)
    if not f.exists():
        logging.error("Path does not exist")
        exit()
    if not f.is_file():
        logging.error("Path provided does not point to a file")
        exit()
    chunk_file(f, output_dir, args.chunksize)


if __name__ == "__main__":
    main()
