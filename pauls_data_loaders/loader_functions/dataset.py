# import libraries
import pandas as pd

def load_load_data(mode='grouped'):
    load_history_raw = pd.read_csv("Load_history.csv")

    if mode == 'raw':
        return load_history_raw

    # step 1: prep the data for unpivoting

    # init empty df to hold prepped data
    load_data_wide = pd.DataFrame()
    
    # copy over the zone_id column (it doesn't need any prepping)
    load_data_wide['zone_id'] = load_history_raw['zone_id']
    
    # convert year, month, day columns to one date column
    load_data_wide['date'] = pd.to_datetime(load_history_raw[['year', 'month', 'day']])
    
    # bring in the hour columns
    hour_columns = [f'h{i}' for i in range(1, 25)]
    for col in hour_columns:
        load_data_wide[col] = load_history_raw[col]

    if mode == 'wide':
        return load_data_wide
    
    # step 2: unpivoting
    load_data_long = load_data_wide.melt(
        id_vars = ['zone_id', 'date'], # cols to preserve
        value_vars=hour_columns,       # cols to unpivot
        var_name='hour',               # colname for new index col (hour)
        value_name='load'              # colname for new value col (load)
    )
    
    # step 3: clean up after unpivot
    
    # convert 'hour' from string (i.e. 'h1') to numeric (i.e. 1)
    load_data_long['hour'] = load_data_long['hour'].str.extract(r'(\d+)').astype(int)
    
    # create a full datetime column by augmenting 'date' with 'hour'
    load_data_long['datetime'] = load_data_long['date'] + pd.to_timedelta(load_data_long['hour'] - 1, unit='h')

    # change dtype on 'load'
    load_data_long['load'] = (
        load_data_long['load']
        .replace(',', '', regex=True)                             # remove commas
        .apply(lambda x: x.strip() if isinstance(x, str) else x)  # strip any whitespace
        .apply(pd.to_numeric, errors='coerce')                    # convert to numeric (keeping NaNs)
    )
    
    # drop extra columns
    load_data_long = load_data_long[['zone_id', 'datetime', 'load']]

    # set datetime as index
    load_data_long.set_index('datetime', inplace=True)

    if mode == 'long':
        return load_data_long

    # step 4: repivot

    # pivot s.t. there is one column per zone
    load_data_grouped = load_data_long.pivot(columns='zone_id', values='load')

    # rename columns for clarity
    load_data_grouped = load_data_grouped.rename(columns=lambda col: f"zone_{col}" if col != 'datetime' else col)
    
    if mode == 'grouped':
        return load_data_grouped

    raise 'InvalidModeError'
    return None

def load_temp_data(mode='grouped'):
    temp_history_raw = pd.read_csv("temperature_history.csv")

    if mode == 'raw':
        return temp_history_raw
    
    # step 1: prep the data for unpivoting

    # init empty df to hold prepped data
    temp_data_wide = pd.DataFrame()
    
    # copy over the zone_id column (it doesn't need any prepping)
    temp_data_wide['station_id'] = temp_history_raw['station_id']
    
    # convert year, month, day columns to one date column
    temp_data_wide['date'] = pd.to_datetime(temp_history_raw[['year', 'month', 'day']])
    
    # bring in the hour columns
    hour_columns = [f'h{i}' for i in range(1, 25)]
    for col in hour_columns:
        temp_data_wide[col] = temp_history_raw[col]

    if mode == 'wide':
        return temp_data_wide
    
    # step 2: unpivoting
    temp_data_long = temp_data_wide.melt(
        id_vars = ['station_id', 'date'], # cols to preserve
        value_vars=hour_columns,          # cols to unpivot
        var_name='hour',                  # colname for new index col (hour)
        value_name='temp'                 # colname for new value col (temp)
    )
    
    # step 3: clean up after unpivot
    
    # convert 'hour' from string (i.e. 'h1') to numeric (i.e. 1)
    temp_data_long['hour'] = temp_data_long['hour'].str.extract(r'(\d+)').astype(int)
    
    # create a full datetime column by augmenting 'date' with 'hour'
    temp_data_long['datetime'] = temp_data_long['date'] + pd.to_timedelta(temp_data_long['hour'] - 1, unit='h')
    
    # drop extra columns
    temp_data_long = temp_data_long[['station_id', 'datetime', 'temp']]

    # set datetime as index
    temp_data_long.set_index('datetime', inplace=True)

    if mode == 'long':
        return temp_data_long
    
    # step 4: repivot

    # pivot s.t. there is one column per zone
    temp_data_grouped = temp_data_long.pivot(columns='station_id', values='temp')

    # rename columns for clarity
    temp_data_grouped = temp_data_grouped.rename(columns=lambda col: f"station_{col}" if col != 'datetime' else col)
    
    if mode == 'grouped':
        return temp_data_grouped

    raise 'InvalidModeError'
    return None

def load_all_data(dropna=True):
    # load datasets
    load_data = load_load_data()
    temp_data = load_temp_data()

    # merge datasets
    merged_data = pd.merge(load_data, temp_data, left_index=True, right_index=True, how='inner')
    if dropna:
        merged_data = merged_data.dropna()

    return merged_data

def load_zone_data(zone, dropna=True, data=None):
    if zone < 1 or zone > 20:
        raise 'DomainError'
        return None

    # make colname list
    station_cols = [f'station_{z}' for z in range(1, 12)]
    cols = [f'zone_{zone}'] + station_cols

    # case 1: no preloaded data
    if data is None:
        # load data
        load_data = load_load_data()
        load_data = load_data[f'zone_{zone}']

        # temp data
        temp_data = load_temp_data()

        # merge
        merged_data = pd.merge(load_data, temp_data, left_index=True, right_index=True, how='inner')

        # make empty df
        final_data = pd.DataFrame()

        # rename & load
        final_data['load'] = merged_data[f'zone_{zone}']

        # load the rest
        for col in station_cols:
            final_data[col] = merged_data[col]

        # dropna
        if dropna:
            final_data = final_data.dropna()

        return final_data
    # case 2: preloaded data
    else:
        # make empty df
        final_data = pd.DataFrame()

        # rename & load
        final_data['load'] = data[f'zone_{zone}']

        # load the rest
        for col in station_cols:
            final_data[col] = data[col]

        # dropna
        if dropna:
            final_data = final_data.dropna()

        return final_data
    
def load_weights():
    weights_raw = pd.read_csv("weights.csv")
    
    # step 1: prep the data for unpivoting

    # init empty df to hold prepped data
    weights_wide = pd.DataFrame()
    
    # copy over the zone_id column (it doesn't need any prepping)
    weights_wide['zone_id'] = weights_raw['zone_id']
    
    # convert year, month, day columns to one date column
    weights_wide['date'] = pd.to_datetime(weights_raw[['year', 'month', 'day']])
    
    # bring in the hour columns
    hour_columns = [f'h{i}' for i in range(1, 25)]
    for col in hour_columns:
        weights_wide[col] = weights_raw[col]

    # don't bring over the id column (it's useless)

    # step 2: unpivoting
    weights_long = weights_wide.melt(
        id_vars = ['zone_id', 'date'], # cols to preserve
        value_vars=hour_columns,          # cols to unpivot
        var_name='hour',                  # colname for new index col (hour)
        value_name='weight'                 # colname for new value col (weight)
    )

    # step 3: clean up after unpivot
    
    # convert 'hour' from string (i.e. 'h1') to numeric (i.e. 1)
    weights_long['hour'] = weights_long['hour'].str.extract(r'(\d+)').astype(int)
    
    # create a full datetime column by augmenting 'date' with 'hour'
    weights_long['datetime'] = weights_long['date'] + pd.to_timedelta(weights_long['hour'] - 1, unit='h')

    # change dtype on 'load'
    weights_long['weight'] = (
        weights_long['weight']
        .replace(',', '', regex=True)                             # remove commas
        .apply(lambda x: x.strip() if isinstance(x, str) else x)  # strip any whitespace
        .apply(pd.to_numeric, errors='coerce')                    # convert to numeric (keeping NaNs)
    )
    
    # drop extra columns
    weights_long = weights_long[['zone_id', 'datetime', 'weight']]

    # set datetime as index
    weights_long.set_index('datetime', inplace=True)

    # step 4: repivot

    # pivot s.t. there is one column per zone
    weights_grouped = weights_long.pivot(columns='zone_id', values='weight')

    # rename columns for clarity
    weights_grouped = weights_grouped.rename(columns=lambda col: f"zone_{col}" if col != 'datetime' else col)

    return weights_grouped