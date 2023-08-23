from pathlib import Path

from mms_monthly_cli.mms_monthly import get_and_unzip_table_csv

get_and_unzip_table_csv(
    2023, 7, "DATA", "DISPATCHABLEUNIT", Path("data", "raw")
)
for year in range(2013, 2022, 1):
    get_and_unzip_table_csv(
        year, 6, "DATA", "BIDPEROFFER", Path("data", "raw")
    )
