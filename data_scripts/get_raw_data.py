from pathlib import Path

from mms_monthly_cli.mms_monthly import get_and_unzip_table_csv

for year in range(2013, 2023, 2):
    get_and_unzip_table_csv(
        year, 7, "DATA", "BIDPEROFFER", Path("data", "raw")
    )
