from pathlib import Path

import pandas as pd
from nem_bidding_dashboard import fetch_and_preprocess, query_cached_data


def populate_raw_cache(raw_cache: Path):
    for year in (2021, 2023):
        fetch_and_preprocess.bid_data(
            start_time=f"{year}/06/04 00:00:00",
            end_time=f"{year}/06/05 00:05:00",
            raw_data_cache=raw_cache,
        )


def process_data(raw_cache: Path, processed_cache: Path):
    """
    Use NEM bidding dashboard to process bidding, dispatch and price data
    from 2021 and 2023.
    Code as per code in example below:
    https://nem-bidding-dashboard.readthedocs.io/en/latest/examples.html#getting-the-data-behind-the-web-app-visualisations
    """
    for year in (2021, 2023):
        start_time = f"{year}/06/04 00:00:00"
        end_time = f"{year}/06/05 00:05:00"
        regions = ["QLD", "NSW", "VIC", "SA", "TAS"]
        dispatch_type = "Generator"
        tech_types = ["Battery Discharge"]
        resolution = "5-min"
        adjusted = "adjusted"
        dispatch_data_column = "AVAILABILITY"
        agg_bids = query_cached_data.aggregate_bids(
            raw_cache,
            start_time,
            end_time,
            regions,
            dispatch_type,
            tech_types,
            resolution,
            adjusted,
        )
        agg_bids.to_csv(
            processed_cache / Path(f"agg_bess_bid_data_{year}0604.csv"),
            index=False,
        )

        dispatch_data = query_cached_data.aggregated_dispatch_data(
            raw_cache,
            dispatch_data_column,
            start_time,
            end_time,
            regions,
            dispatch_type,
            tech_types,
            resolution,
        )
        dispatch_data = dispatch_data.rename(
            columns={
                "INTERVAL_DATETIME": "SETTLEMENTDATE",
                "COLUMNVALUES": dispatch_data_column,
            }
        )
        region_demand = query_cached_data.region_demand(
            raw_cache, start_time, end_time, regions
        )
        aggregated_vwap = query_cached_data.aggregated_vwap(
            raw_cache, start_time, end_time, regions
        )
        dispatch_data = pd.merge(
            dispatch_data, region_demand, on="SETTLEMENTDATE"
        )
        dispatch_data = pd.merge(
            dispatch_data, aggregated_vwap, on="SETTLEMENTDATE"
        )
        dispatch_data.to_csv(
            processed_cache / Path(f"agg_bess_dispatch_data_{year}0604.csv"),
            index=False,
        )


if __name__ == "__main__":
    raw_data_cache = Path("data", "raw")
    processed_data_cache = Path("data", "processed")
    populate_raw_cache(raw_data_cache)
    process_data(raw_data_cache, processed_data_cache)
