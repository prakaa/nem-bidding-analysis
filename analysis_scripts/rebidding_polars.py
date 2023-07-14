# %%
from datetime import datetime
from timeit import default_timer

import polars as pl

start = default_timer()
# %%
print("Parquet query")
q = pl.scan_parquet("BIDPEROFFER_basic/*.parquet").filter(
    (
        (pl.col("TRADINGDATE") == datetime(2021, 7, 7))
        & (pl.col("PERIODID").is_between(200, 224, closed="both"))
    )
)
df_q = q.collect()
df_q.to_pandas()
t1 = default_timer()
parquet_t = t1 - start
print(f"{parquet_t} seconds for parquet query")

# %%
print("Parquet query on TRADINGDATE partioned data")
q1 = pl.scan_parquet(
    "BIDPEROFFER_tradingdate_1000000/20210707*.parquet"
).filter((pl.col("PERIODID").is_between(200, 224, closed="both")))
q1.collect()
df_q1 = q1.collect()
df_q1.to_pandas()
t2 = default_timer()
parquet_2_t = t2 - t1
print(f"{parquet_2_t} seconds for parquet query")


# %%
print("CSV query")
q2 = pl.scan_csv(
    "./PUBLIC_DVD_BIDPEROFFER_202107010000.CSV", skip_rows=1
).filter(
    (pl.col("TRADINGDATE") == "2021/07/07 00:00:00")
    & (pl.col("PERIODID").is_between(200, 224, closed="both"))
)
q2.collect()
df_q2 = q2.collect()
df_q2.to_pandas()
t3 = default_timer()
csv_t = t3 - t2
print(f"{csv_t} seconds for csv query")
