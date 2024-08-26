# %%
from pathlib import Path
from typing import List

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
from nem_bidding_dashboard.defaults import bid_order
from plotly.subplots import make_subplots

pio.kaleido.scope.mathjax = None
data_path = Path("data", "processed")

plotly_template = dict(
    layout=go.Layout(
        font_family="Ubuntu",
        title_font_size=24,
        title_x=0.05,
        plot_bgcolor="#f0f0f0",
        colorway=px.colors.qualitative.Bold,
        xaxis={"griddash": "dash"},
        yaxis={"griddash": "dash"},
    )
)
divergent_colors = [
    "#313695",
    "#4575b4",
    "#FFFFFF",
    "#ffffbf",
    "#fee090",
    "#fdae61",
    "#f46d43",
    "#941a30",
]


def consolidate_bid_buckets(bid_order: List[str]):
    bid_order.remove("[0, 50)")
    bid_order.remove("[50, 100)")
    bid_order.insert(2, "[0, 100)")
    bid_order.remove("[100, 200)")
    bid_order.remove("[200, 300)")
    bid_order.insert(3, "[100, 300)")
    bid_order.remove("[1000, 5000)")
    bid_order.remove("[5000, 10000)")
    bid_order.insert(6, "[1000, 10000)")
    return bid_order


def consolidate_bid_buckets_in_data(bid_df: pd.DataFrame):
    bid_df.BIN_NAME.replace("[0, 50)", "[0, 100)", inplace=True)
    bid_df.BIN_NAME.replace("[50, 100)", "[0, 100)", inplace=True)
    bid_df.BIN_NAME.replace("[100, 200)", "[100, 300)", inplace=True)
    bid_df.BIN_NAME.replace("[200, 300)", "[100, 300)", inplace=True)
    bid_df.BIN_NAME.replace("[1000, 5000)", "[1000, 10000)", inplace=True)
    bid_df.BIN_NAME.replace("[5000, 10000)", "[1000, 10000)", inplace=True)
    return bid_df


# %%
bid_df_2021 = pd.read_csv(Path(data_path, "agg_bess_bid_data_20210604.csv"))
dispatch_df_2021 = pd.read_csv(
    Path(data_path, "agg_bess_dispatch_data_20210604.csv")
)
bid_df_2023 = pd.read_csv(Path(data_path, "agg_bess_bid_data_20230604.csv"))
dispatch_df_2023 = pd.read_csv(
    Path(data_path, "agg_bess_dispatch_data_20230604.csv")
)
consolidated_bid_order = consolidate_bid_buckets(bid_order)
bin_df_2021 = consolidate_bid_buckets_in_data(bid_df_2021)
bin_df_2023 = consolidate_bid_buckets_in_data(bid_df_2023)

fig = make_subplots(
    rows=2,
    specs=[[dict(secondary_y=True)], [dict(secondary_y=True)]],
    subplot_titles=["June 4, 2021", "June 4, 2023"],
)

for bid_band, color in zip(consolidated_bid_order, divergent_colors):
    bid_band_df_2021 = bid_df_2021.loc[bid_df_2021.BIN_NAME == bid_band, :]

    # If the bin is missing in 2021 data, add a zero-height bar to include in legend
    if bid_band_df_2021.empty:
        fig.add_trace(
            go.Bar(
                x=[None],
                y=[0],
                marker=dict(color=color),
                name=bid_band,
                legendgroup="price",
                legendgrouptitle=dict(text="Offer price (AUD/MW/hr)"),
            ),
            row=1,
            col=1,
        )
    else:
        fig.add_trace(
            go.Bar(
                x=bid_band_df_2021.INTERVAL_DATETIME,
                y=bid_band_df_2021.BIDVOLUME,
                marker=dict(color=color),
                name=bid_band,
                legendgroup="price",
                legendgrouptitle=dict(text="Offer price (AUD/MW/hr)"),
            ),
            row=1,
            col=1,
        )

for bid_band, color in zip(consolidated_bid_order, divergent_colors):
    bid_band_df = bid_df_2023.loc[bid_df_2023.BIN_NAME == bid_band, :]
    fig.add_trace(
        go.Bar(
            x=bid_band_df.INTERVAL_DATETIME,
            y=bid_band_df.BIDVOLUME,
            marker=dict(color=color),
            name=bid_band,
            legendgroup="price",
            showlegend=False,
        ),
        row=2,
        col=1,
    )

fig.add_trace(
    go.Scatter(
        name="Average price (AUD/MW/hr)",
        x=dispatch_df_2021.SETTLEMENTDATE,
        y=dispatch_df_2021.PRICE,
        marker=dict(color="black"),
        line=dict(width=1),
        legendgroup="vwap",
        legendgrouptitle=dict(text="NEM-wide (volume-weighted)"),
    ),
    secondary_y=True,
    row=1,
    col=1,
)

fig.add_trace(
    go.Scatter(
        name="Average price",
        x=dispatch_df_2023.SETTLEMENTDATE,
        y=dispatch_df_2023.PRICE,
        marker=dict(color="black"),
        line=dict(width=1),
        showlegend=False,  # Prevents repetition in the second subplot
    ),
    secondary_y=True,
    row=2,
    col=1,
)

fig.update_xaxes(showgrid=False)
fig.update_yaxes(showgrid=False)
fig.update_layout(
    height=450,
    width=700,
    barmode="stack",
    bargap=0,
    template=plotly_template,
    title="NEM-wide Aggregate Volume of BESS Offers by Price",
    legend=dict(
        xanchor="right",
        x=1.6,
    ),
)

for row in (1, 2):
    fig.update_yaxes(title_text="Volume (MW)", row=row, secondary_y=False)
    fig.update_yaxes(title_text="Price (AUD/MW/hr)", row=row, secondary_y=True)
fig.write_image(Path("plots", "aggregate_bess_bidding_0406_2021_2023.pdf"))
