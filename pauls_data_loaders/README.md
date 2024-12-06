PAUL'S DATA LOADERS

I: Single File Dataset Loaders

a. load_load_data(mode='grouped')
    USE:                loads data from "Load_history.csv"
    mode (optional):    determines how much preprocessing it does. 
        'raw':          loads and outputs the file with no preprocessing
        'wide':         [ 'zone_id', 'date', 'h1'...'h24' ]
        'long':         [ 'zone_id', 'load' ] (datetime as index)
        'grouped':      [ 'zone_1'...'zone_20' ] (datetime as index, load as values) (recommended)
    OUTPUT:             DataFrame containing load data

b. load_temp_data(mode='grouped')
    USE:                loads data from "temperature_history.csv"
    mode (optional):    determines how much preprocessing it does. 
        'raw':          loads and outputs the file with no preprocessing
        'wide':         [ 'station_id', 'date', 'h1'...'h24' ]
        'long':         [ 'station_id', 'temp' ] (datetime as index)
        'grouped':      [ 'station_1'...'station_11' ] (datetime as index, temp as values) (recommended)
    OUTPUT:             DataFrame containing temp data

c. load_weights()
    OUTPUT:             DataFrame containing weights for WRMSE in 'grouped' format

II: Multiple File Dataset Loaders

a. load_all_data(dropna=True):
    USE:                loads load and temp data in one DataFrame
    dropna (optional):  drops any rows with missing data when True
    OUTPUT:             DataFrame containing merged load and temp data in 'grouped' format

b. load_zone_data(zone, dropna=True, data=None)
    USE:                loads load data for one zone and temp data for all stations in one DataFrame
    dropna (optional):  drops any rows with missing data when True
    data (optional):    feed in the output from load_all_data to skip reloading from scratch (recommended)
    OUTPUT:             DataFrame containing load data for one zone and temp data for all stations