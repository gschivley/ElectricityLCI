# -*- coding: utf-8 -*-
import pandas as pd
from electricitylci.globals import output_dir, data_dir
import electricitylci.alt_generation as altg
import electricitylci.import_impacts as import_impacts
from electricitylci.model_config import eia_gen_year


def fill_nans(df, key_column = "FacilityID", target_columns=[],dropna=True):
    if not target_columns:
        target_columns=[
                "Balancing Authority Code",
                "Balancing Authority Name",
                "FRS_ID",
                "FuelCategory",
                "NERC",
                "PrimaryFuel",
                "PercentGenerationfromDesignatedFuelCategory",
                "eGRID_ID",
                "Subregion"
                ]
    key_df = df[[key_column]+target_columns].drop_duplicates(subset="FacilityID").set_index("FacilityID")
    for col in target_columns:
        df.loc[df[col].isnull(),col]=df.loc[df[col].isnull(),"FacilityID"].map(key_df[col])
    if dropna:
        df.dropna(subset=target_columns,inplace=True)
    return df

def concat_map_upstream_databases(*arg):
    """
    Concatenates all of the upstream databases given as args. Then all of the
    emissions in the combined database are mapped to the federal elementary
    flows list in preparation for being turned into openLCA processes and 
    combined with the generation emissions.

    Parameters
    ----------
    *arg : dataframes
        The dataframes to be combined, generated by the upstream modules (
        electricitylci.nuclear_upstream, .petroleum_upstream, .coal_upstream,
        .natural_gas_upstream.)
    
    Returns
    -------
    datafame
    """
    mapped_column_dict = {
        #        "UUID (EPA)": "FlowUUID",
        #         "FlowName": "model_flow_name",
        #        "Flow name (EPA)": "FlowName",
        "Flowable": "FlowName",
        "Flow UUID": "FlowUUID",
        "Compartment_path_y": "Compartment_path",
    }
    compartment_mapping = {
        "air": "emission/air",
        "water": "emission/water",
        "ground": "emission/ground",
        "soil": "emission/soil",
        "resource": "resource",
    }
    print(f"Concatenating and flow-mapping {len(arg)} upstream databases.")
    upstream_df_list = list()
    for df in arg:
        upstream_df_list.append(df)
    upstream_df = pd.concat(upstream_df_list, ignore_index=True, sort=False)
    upstream_df["Compartment_path"] = upstream_df["Compartment"].map(
        compartment_mapping
    )
    netl_epa_flows = pd.read_csv(
        data_dir + "/netl_fedelem_crosswalk.csv",
        header=0,
        usecols=[
            1,
            4,
            9,
            13,
            19,
            17,
        ],  # FlowName_netl, Compartment_path_netl, CAS, Compartment_path, Flowable, Flow UUID
    )

    upstream_df = upstream_df.groupby(
        [
            "fuel_type",
            "stage_code",
            "FlowName",
            "Compartment",
            "plant_id",
            "Compartment_path",
        ],
        as_index=False,
    ).agg({"FlowAmount": "sum", "quantity": "mean", "Electricity": "mean"})
    upstream_mapped_df = pd.merge(
        left=upstream_df,
        right=netl_epa_flows,
        left_on=["FlowName", "Compartment_path"],
        right_on=["FlowName_netl", "Compartment_path_netl"],
        how="left",
    )
    upstream_mapped_df.drop(
        columns=[
            "Compartment_path_x",
            "FlowName",
            "Compartment_path_x",
            "Compartment_path_netl",
            "Compartment_path_netl",
        ],
        inplace=True,
    )
    upstream_mapped_df = upstream_mapped_df.rename(
        columns=mapped_column_dict, copy=False
    )
    upstream_mapped_df.drop_duplicates(
        subset=["plant_id","FlowName", "Compartment_path", "FlowAmount"], inplace=True
    )
    upstream_mapped_df.dropna(subset=["FlowName"], inplace=True)
    garbage = upstream_mapped_df.loc[
        upstream_mapped_df["FlowName"] == "[no match]", :
    ].index
    upstream_mapped_df.drop(garbage, inplace=True)
    upstream_mapped_df.rename(
        columns={"fuel_type": "FuelCategory"}, inplace=True
    )
    upstream_mapped_df["FuelCategory"] = upstream_mapped_df[
        "FuelCategory"
    ].str.upper()
    upstream_mapped_df["ElementaryFlowPrimeContext"] = "emission"
    upstream_mapped_df["Unit"] = "kg"
    upstream_mapped_df["Source"] = "netl"
    upstream_mapped_df["Year"] = eia_gen_year
    return upstream_mapped_df


def concat_clean_upstream_and_plant(pl_df, up_df):
    """
    Combined the upstream and the generator (power plant) databases followed
    by some database cleanup
    
    Parameters
    ----------
    pl_df : dataframe
        The generator dataframe, generated by electricitylci.generation or
        .alt_generation
    up_df : dataframe
        The combined upstream dataframe.
    
    Returns
    -------
    dataframe
    """
    region_cols = [
        "NERC",
        "Balancing Authority Code",
        "Balancing Authority Name",
        "Subregion",
    ]
    up_df = up_df.merge(
        right=pl_df[["eGRID_ID"] + region_cols].drop_duplicates(),
        left_on="plant_id",
        right_on="eGRID_ID",
        how="left",
    )
    up_df.dropna(subset=region_cols + ["Electricity"], inplace=True)
    combined_df = pd.concat([pl_df, up_df], ignore_index=True)
    combined_df.drop(columns=["plant_id"], inplace=True)
    combined_df["FacilityID"] = combined_df["eGRID_ID"]
    combined_df = fill_nans(combined_df)
    return combined_df


def add_fuel_inputs(gen_df, upstream_df, upstream_dict):
    """
    Converts the upstream emissions database to fuel inputs and adds them
    to the generator dataframe. This is in preparation of generating unit
    processes for openLCA.
    Parameters
    ----------
    gen_df : dataframe
        The generator df containing power plant emissions.
    upstream_df : dataframe
        The combined upstream dataframe.
    upstream_dict : dictionary
        This is the dictionary of upstream "unit processes" as generated by
        electricitylci.upstream_dict after the upstream_dict has been written
        to json-ld. This is important because the uuids for the upstream
        "unit processes" are only generated when written to json-ld.
    
    Returns
    -------
    dataframe
    """
    from electricitylci.generation import (
        add_technological_correlation_score,
        add_temporal_correlation_score,
    )

    upstream_reduced = upstream_df.drop_duplicates(
        subset=["plant_id", "stage_code", "quantity"]
    )
    fuel_df = pd.DataFrame(columns=gen_df.columns)
    # The upstream reduced should only have one instance of each plant/stage code
    # combination. We'll first map the upstream dictionary to each plant
    # and then expand that dictionary into columns we can use. The goal is
    # to generate the fuels and associated metadata with each plant. That will
    # then be merged with the generation database.
    fuel_df["flowdict"] = upstream_reduced["stage_code"].map(upstream_dict)

    expand_fuel_df = fuel_df["flowdict"].apply(pd.Series)
    fuel_df.drop(columns=["flowdict"], inplace=True)

    fuel_df["Compartment"] = "input"
    fuel_df["FlowName"] = expand_fuel_df["q_reference_name"]
    fuel_df["stage_code"] = upstream_reduced["stage_code"]
    fuel_df["FlowAmount"] = upstream_reduced["quantity"]
    fuel_df["FlowUUID"] = expand_fuel_df["q_reference_id"]
    fuel_df["Unit"] = expand_fuel_df["q_reference_unit"]
    fuel_df["eGRID_ID"] = upstream_df["plant_id"]
    fuel_df["FacilityID"] = upstream_df["plant_id"]
    fuel_df["FuelCategory"] = upstream_df["FuelCategory"]
    fuel_df["Year"] = upstream_df["Year"]
    merge_cols = [
        "Age",
        "Balancing Authority Code",
        "Balancing Authority Name",
        "Electricity",
        "FRS_ID",
        "NERC",
        "Subregion",
    ]
    fuel_df.drop(columns=merge_cols, inplace=True)
    gen_df_reduced = gen_df[merge_cols + ["eGRID_ID"]].drop_duplicates(
        subset=["eGRID_ID"]
    )

    fuel_df = fuel_df.merge(
        right=gen_df_reduced,
        left_on="eGRID_ID",
        right_on="eGRID_ID",
        how="left",
    )
    fuel_df.dropna(subset=["FRS_ID"], inplace=True)
    fuel_df["Source"] = "eia"
    fuel_df = add_temporal_correlation_score(fuel_df)
    fuel_df["DataCollection"] = 5
    fuel_df["GeographicalCorrelation"] = 1
    fuel_df["TechnologicalCorrelation"] = 1
    fuel_df["ReliabilityScore"] = 1
    fuel_df["ElementaryFlowPrimeContext"] = "input"
    gen_plus_up_df = pd.concat([gen_df, fuel_df], ignore_index=True)
    return gen_plus_up_df


if __name__ == "__main__":
    import electricitylci.coal_upstream as coal
    import electricitylci.natural_gas_upstream as ng
    import electricitylci.petroleum_upstream as petro
    import electricitylci.geothermal as geo
    import electricitylci.solar_upstream as solar
    import electricitylci.wind_upstream as wind
    import electricitylci.nuclear_upstream as nuke

    coal_df = coal.generate_upstream_coal(2016)
    ng_df = ng.generate_upstream_ng(2016)
    petro_df = petro.generate_petroleum_upstream(2016)
    geo_df = geo.generate_upstream_geo(2016)
    solar_df = solar.generate_upstream_solar(2016)
    wind_df = wind.generate_upstream_wind(2016)
    nuke_df = nuke.generate_upstream_nuc(2016)
    upstream_df = concat_map_upstream_databases(
        coal_df, ng_df, petro_df, geo_df, nuke_df, solar_df, wind_df
    )
    plant_df = altg.create_generation_process_df()
    plant_df["stage_code"] = "Power plant"
    combined_df = concat_clean_upstream_and_plant(plant_df, upstream_df)
    canadian_inventory = import_impacts.generate_canadian_mixes(combined_df)
    combined_df = pd.concat([combined_df, canadian_inventory])
    combined_df.sort_values(
        by=["eGRID_ID", "Compartment", "FlowName", "stage_code"], inplace=True
    )
    combined_df.to_csv(f"{output_dir}/combined_df.csv")
