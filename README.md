# NEM (Re)Bidding Analysis, 2012-2021

(Re)bidding analysis of the Australian National Electricity Market (NEM), as well as tools that help with analysing bidding data from the NEM.

This repository is a companion to the pre-dispatch forecast analyses in [this respository](https://github.com/prakaa/NEMStorageUnderUncertainty).

## Results

### Bidding zip file size, 2012-2021

Monthly bidding data zip file sizes have increased exponentially since the middle of 2021. See (this plot)[./plots/monthly_bidding_data_size_2012_2023.pdf].

### Share of rebids by technology type in June, 2013-2021

These plots look at the share of rebids within a certain time of delivery (i.e. within a certain number of minutes of the delivery dispatch interval) across years by technology type:

- [Within 5 minutes of dispatch](./plots/rebids_june_300_share_by_tech_2013_2021.pdf)
- [Within 60 minutes of dispatch](./plots/rebids_june_3600_share_by_tech_2013_2021.pdf)

## Usage

### Installation

Install [`poetry`](https://python-poetry.org/) and run the following install this repository to create a `poetry` virtual environment:

```bash
poetry install
```

## Running analysis and plotting scripts

This repository uses a [Makefile](./Makefile) to automate a series of Python scripts used for the analysis.

Processed data has already been committed to this repository. To plot the results in the [`plots`](./plots/) directory, run:

```bash
make create_plots
```

If you wish to change what is being analysed (e.g. different years, different months), you will need to process the data yourself.

To get bidding zip file size data, run the following:

```bash
make create_data_for_bid_zip_file_plot
```
To get rebid data with a certain ahead time for a particular month across years, run the command below.

The scripts within this pipeline use [polars](https://www.pola.rs/) to manage memory, but it will still need a machine with 20-25 GB RAM.

```bash
make create_data_for_rebid_plots
```

## Author & licenses

This repository contains work by Abhijith (Abi) Prakash, PhD Candidate at the UNSW Collaboration on Energy and Environmental Markets. If you are interested in extending or using this work, please get in touch. The source code is currently light on documentation.

The source code from this work is licensed under the terms of [GNU GPL-3.0-or-later licences](./LICENSE). It includes modified [source code](https://unsw-ceem.github.io/CEEM-Gists/opennem_facilities.html) made available by Dylan McConnell under the terms of GNU GPL-3.0-or-later.

The results (generated plots) are licensed under a [Creative Commons Attribution 4.0 International License](http://creativecommons.org/licenses/by/4.0/).

## Acknowledgements

- Nicholas Gorman for assistance in developing pipeline to analyse bidding data
