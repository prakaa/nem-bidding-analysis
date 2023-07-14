import argparse
import logging
from pathlib import Path

import pandas as pd
from nemosis import data_fetch_methods as data_fetch_methods


def fetch_gen_scheduled_loads(raw_loc, table_loc):
    """
    Fetches the Registration and Exemptions xlsx and returns the Generators
    and Scheduled Loads table

    Args:
        raw_loc (str or path): directory to save the raw xlsx
        table_loc (str or path): directory to save the G&L table as a csv

    Returns:
        Pandas DataFrame of Generators and Scheduled Loads table
    """
    nemosis_table = "Generators and Scheduled Loads"
    df = data_fetch_methods.static_table_xl(
        table_name=nemosis_table, raw_data_location=raw_loc
    )
    df.to_csv(Path(table_loc, "generators_and_loads.csv"), index=False)

    return df


def fetch_ancillary_service_providers(reg_exemps_xlsx_loc, table_loc=None):
    """
    Looks at the Ancillary Services tab of the Registration and Exemptions xlsx
    and returns the Ancillary Service providers table, which includes all
    ancillary services providers

    Args:
        reg_exemps_xlsx_loc (str or path): directory where NEM Reg&Exemp.xlsx
                                           is located.
        table_loc (str or path): directory to save the Ancillary Services table

    Returns:
        Pandas DataFrame of Generators and Scheduled Loads table
    """
    reg_exemps = Path(
        reg_exemps_xlsx_loc, "NEM Registration and Exemption List.xls"
    )
    df = pd.read_excel(reg_exemps, sheet_name="Ancillary Services")

    df.dropna(axis=1, how="all", inplace=True)
    df.drop(columns=["Unnamed: 11"], inplace=True)

    df.to_csv(
        Path(table_loc, "ancillary_service_providers.csv"),
        index=False,
    )

    return df


def clean_gen_loads_tech(
    gen_loads_path=None,
    df=None,
    table_loc=None,
    outname="generators_and_loads.csv",
):
    """
    Registration and Exemptions list contain various technology types
    Some of these are repeated (difference of case or a few words)
    Take generators and scheduled loads csv and
    cleans technology types based on mapping in this function

    Args:
        gen_loads_path (str or pat, optional): Directory containing G&L csv
        df (pd.DataFrame, optional): provide df instead of path to csv
        table_loc (str or path, optional): path to save cleaned table
        outname (str, optional): used if table_loc provided

    Returns:
        Pandas Dataframe with condensed tech types
    """
    condensed_techs = {
        "Battery and Inverter": "Battery",
        "Combined Cycle Gas Turbine (CCGT)": "CCGT",
        "Open Cycle Gas turbines (OCGT)": "OCGT",
        "Run of River": "Hydro - Run of River",
        "Photovoltaic Flat panel": "PV",
        "Photovoltaic Flat Panel": "PV",
        "Photovoltaic Tracking  Flat Panel": "PV",
        "Photovoltaic Tracking Flat Panel": "PV",
        "Photovoltaic Tracking Flat panel": "PV",
        "Wind - Onshore": "Wind",
        "Pump Storage": "Pump/Load",
        "-": "Pump/Load",
    }
    replace = (
        lambda x: condensed_techs[x] if x in condensed_techs.keys() else x
    )
    if gen_loads_path:
        df = pd.read_csv(Path(gen_loads_path, "generators_and_loads.csv"))
    df["Technology Type - Descriptor"] = df[
        "Technology Type - Descriptor"
    ].apply(replace)

    if table_loc:
        csv_path = Path(table_loc, outname)
        df.to_csv(csv_path, index=False)

    return df


def clean_gen_loads_capacities(
    gen_loads_path=None,
    df=None,
    table_loc=None,
    outname="generators_and_loads.csv",
):
    """
    Each row in the Generators and Loads list corresponds to a
    unit. Each unit has various capacities (MW).
    Reg Cap (MW) is believed to correspond to unit capacities.
    This function cleans any null values and returns a
    dataframe with numerical capacities.
    Args:
        gen_loads_path (str or pat, optional): Directory containing G&L csv
        df (pd.DataFrame, optional): provide df instead of path to csv
        table_loc (str or path, optional): path to save cleaned table
        outname (str, optional): used if table_loc provided

    Returns:
        Pandas Dataframe with cleaned capacities
    """
    if gen_loads_path:
        df = pd.read_csv(Path(gen_loads_path, "generators_and_loads.csv"))
    df["Reg Cap (MW)"] = df["Reg Cap (MW)"].str.replace("-", "0")
    df["Reg Cap (MW)"] = df["Reg Cap (MW)"].astype("float64")

    if table_loc:
        csv_path = Path(table_loc, outname)
        df.to_csv(csv_path, index=False)
    return df


def find_non_genloads_duid_providers(
    gen_loads_path, ancillary_services_path, table_loc=None
):
    """
    Some DUIDs map to market ancillary service providers.
    This function identifies these DUIDs and returns
    a dataframe with basic information about the DUID.

    Args:
        gen_loads_path (str or path): Directory containing G&L csv
        ancillary_services_path (str or path): Dir containing ASP csv
        table_loc (str or path, optional): path to save table with providers

    Returns:
        Pandas Dataframe with unique fcas providers and basic info
    """
    asp_path = Path(ancillary_services_path, "ancillary_service_providers.csv")
    gen_load_path = Path(gen_loads_path, "generators_and_loads.csv")
    fcas_providers = pd.read_csv(asp_path)
    gen_load = pd.read_csv(gen_load_path)

    non_genloads_duid = set(fcas_providers["DUID"]) - set(gen_load["DUID"])
    non_genloads_duid_providers = fcas_providers[
        fcas_providers["DUID"].isin(non_genloads_duid)
    ]
    non_genloads_duid_providers = non_genloads_duid_providers[
        ~non_genloads_duid_providers["DUID"].isna()
    ]
    fcas_provider_cols = ["DUID", "Region", "Participant", "Station Name"]
    non_genloads_duid_providers = non_genloads_duid_providers.drop_duplicates(
        fcas_provider_cols
    )
    non_genloads_duid_providers = non_genloads_duid_providers[
        fcas_provider_cols
    ]

    if table_loc:
        save_path = Path(table_loc, "non_genloads_duid_providers.csv")
        non_genloads_duid_providers.to_csv(save_path, index=False)

    return non_genloads_duid_providers


def create_parser():
    description = "Fetch participant tables into project data directories"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "-raw_path",
        type=str,
        required=True,
        help="path to save fetched raw files",
    )
    parser.add_argument(
        "-proc_path",
        type=str,
        required=True,
        help="path to save cleaned files",
    )
    args = parser.parse_args()
    return args


def main():
    logging.basicConfig(
        format="\n%(levelname)s:%(message)s", level=logging.INFO
    )
    args = create_parser()
    raw_path = args.raw_path
    proc_path = args.proc_path
    gen_loads_outname = "cleaned_gen_loads.csv"
    # fetch raw generators and loads and clean, then save to processed path
    raw_gen_loads = fetch_gen_scheduled_loads(raw_path, raw_path)
    cleaned_tech = clean_gen_loads_tech(df=raw_gen_loads)
    clean_gen_loads_capacities(
        df=cleaned_tech, table_loc=proc_path, outname=gen_loads_outname
    )
    logging.info(
        (
            f"Raw Gen and Load files in {raw_path},"
            + f"processed in {proc_path}"
        )
    )
    # fetch fcas providers then find unique providers
    fetch_ancillary_service_providers(raw_path, table_loc=raw_path)
    find_non_genloads_duid_providers(raw_path, raw_path, table_loc=proc_path)
    logging.info(
        f"Non gen loads in {raw_path}, unique non gen loads in {proc_path}"
    )


if __name__ == "__main__":
    main()
