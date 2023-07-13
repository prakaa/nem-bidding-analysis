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
from typing import List

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
strf_format = "%Y%m%d%H%M%S"


def arg_parser():
    description = (
        "Chunk large monthly AEMO data table CSVs into parquet partitions "
        + "using a data column. "
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
        "-partition_col",
        type=str,
        help=("Column to partition parquet files on"),
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


def get_date_cols(columns: pd.Index) -> List[str]:
    return [col for col in columns if "DATE" in col]


def write_chunks_by_trading_date(
    chunk: pd.DataFrame, output_dir: Path, partition_col: str
) -> None:
    unique_values = chunk[partition_col].unique().tolist()
    for value in unique_values:
        value_chunk = chunk.loc[chunk[partition_col] == value, :]
        if type(value) == pd.Timestamp:
            str_value = value.strftime(strf_format)
        else:
            str_value = str(value)
        str_value = str_value.replace("/", "")
        str_value = str_value.replace("\\", "")
        base_file_name = Path(output_dir, str_value + "-chunk-")
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
        value_chunk.to_parquet(filename, engine="pyarrow")
    return None


def chunk_file(
    file_path: Path, output_dir: Path, partition_col: str, chunksize: int
) -> None:
    if not file_path.suffix.lower() == ".csv":
        logging.error("File is not a CSV")
        exit()
    cols = get_columns(file_path)
    if partition_col not in cols:
        logging.error(f"Partition col {partition_col} not in data")
        exit()
    date_cols = get_date_cols(cols)
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
        parse_dates=date_cols,
        date_format=dt_format,
    ) as reader:
        with tqdm(
            total=file_size, desc="Progress estimate based on file size"
        ) as pbar:
            for chunk in reader:
                if previous_chunk is not None:
                    write_chunks_by_trading_date(
                        previous_chunk, output_dir, partition_col
                    )
                previous_chunk = chunk
                # See here for comparison of pandas DataFrame size vs CSV size:
                # https://stackoverflow.com/questions/18089667/how-to-estimate-how-much-memory-a-pandas-dataframe-will-need#32970117
                pbar.update((size_per_line * chunksize) / 2)
            write_chunks_by_trading_date(
                previous_chunk.iloc[:-1],  # type: ignore
                output_dir,
                partition_col=partition_col,
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
    chunk_file(f, output_dir, args.partition_col, args.chunksize)


if __name__ == "__main__":
    main()
