import pandas as pd
import json
from os.path import join

from electricitylci.globals import data_dir


path = join(data_dir, 'bulk_data', 'EBA.txt')
with open(path, 'r') as f:
    raw_txt = f.readlines()

REGION_NAMES = [
    'California', 'Carolinas', 'Central',
    'Electric Reliability Council of Texas, Inc.', 'Florida',
    'Mid-Atlantic', 'Midwest', 'New England ISO',
    'New York Independent System Operator', 'Northwest', 'Southeast',
    'Southwest', 'Tennessee Valley Authority'
]

REGION_ACRONYMS = [
    'TVA', 'MIDA', 'CAL', 'CAR', 'CENT', 'ERCO', 'FLA',
    'MIDW', 'ISNE', 'NYIS', 'NW', 'SE', 'SW',
]

TOTAL_INTERCHANGE_ROWS = [
    json.loads(row) for row in raw_txt if 'EBA.TI.H' in row
]

NET_GEN_ROWS = [
    json.loads(row) for row in raw_txt if 'EBA.NG.H' in row
]

DEMAND_ROWS = [
    json.loads(row) for row in raw_txt if 'EBA.D.H' in row
]

EXCHANGE_ROWS = [
    json.loads(row) for row in raw_txt if 'EBA.ID.H' in row
]

BA_TO_BA_ROWS = [
    row for row in EXCHANGE_ROWS
    if row['series_id'].split('-')[0][4:] not in REGION_ACRONYMS
]


def row_to_df(rows, data_type):
    """
    Turn rows of a single type from the bulk data text file into a dataframe
    with the region, datetime, and data as columns

    Parameters
    ----------
    rows : list
        rows from the EBA.txt file
    data_type : str
        name to use for the data column (e.g. demand or total_interchange)

    Returns
    -------
    dataframe
        Data for all regions in a single df with datatimes converted and UTC
    """
    df_list = []
    for row in rows:

        # "data" is of form:
        # [['20190214T04Z', -102],
        # ['20190214T03Z', -107],
        # ['20190214T02Z', -108],
        # ['20190214T01Z', -103]]
        datetime = pd.to_datetime([x[0] for x in row['data']],
                                  utc=True, format='%Y%m%dT%HZ')
        data = [x[1] for x in row['data']]
        region = row['series_id'].split('-')[0][4:]

        df_data = {
            'region': region,
            'datetime': datetime,
            data_type: data,
        }

        _df = pd.DataFrame(df_data)
        df_list.append(_df)

    df = pd.concat(df_list).reset_index(drop=True)

    return df


def ba_exchange_to_df(rows, data_type='ba_to_ba'):
    """
    Turn rows of a single type from the bulk data text file into a dataframe
    with the region, datetime, and data as columns

    Parameters
    ----------
    rows : list
        rows from the EBA.txt file
    data_type : str
        name to use for the data column (e.g. demand or total_interchange)

    Returns
    -------
    dataframe
        Data for all regions in a single df with datatimes converted and UTC
    """
    df_list = []
    for row in rows:

        # "data" is of form:
        # [['20190214T04Z', -102],
        # ['20190214T03Z', -107],
        # ['20190214T02Z', -108],
        # ['20190214T01Z', -103]]
        datetime = pd.to_datetime([x[0] for x in row['data']],
                                  utc=True, format='%Y%m%dT%HZ')
        data = [x[1] for x in row['data']]
        from_region = row['series_id'].split('-')[0][4:]
        to_region = row['series_id'].split('-')[1][:-5]

        df_data = {
            'from_region': from_region,
            'to_region': to_region,
            'datetime': datetime,
            data_type: data,
        }

        _df = pd.DataFrame(df_data)
        df_list.append(_df)

    df = pd.concat(df_list).reset_index(drop=True)

    return df
