from pathlib import Path

from create_parquet_partitions_by_column import chunk_file, get_columns

raw_csvs = sorted(Path.glob(Path("data", "raw"), "*_BIDPEROFFER_*.CSV"))
if not (output_dir := Path("data", "partitioned")).exists():
    output_dir.mkdir()
for csv in raw_csvs:
    cols = get_columns(csv)
    if "TRADINGDATE" in cols:
        partition_col = "TRADINGDATE"
    elif "SETTLEMENTDATE" in cols:
        partition_col = "SETTLEMENTDATE"
    partition_dir = output_dir / Path(partition_col)
    if not partition_dir.exists():
        partition_dir.mkdir()
    chunk_file(csv, partition_dir, partition_col, chunksize=10**6)
    csv.unlink()
