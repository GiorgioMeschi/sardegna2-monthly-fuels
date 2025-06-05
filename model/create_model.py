
#%%
import os
import rasterio as rio
import multiprocessing as mp
import logging

from annual_wildfire_susceptibility.supranational_model import SupranationalModel

from risico_operational.settings import DATAPATH, TILES_DIR

#%%

VS = 'v3'
CONFIG = {     
    "batches" : 1, 
    "nb_codes_list" : [1],
    "list_features_to_remove" : ["lat", "lon", "veg_0"],
    "convert_to_month" : 1, # we turn on here here the month setting 
    "aggr_seasonal": 0, # we turn off the seasonal analysis.
    "wildfire_years" : [2011, 2012, 2013, 2014, 2015, 2015, 2017, 2018, 2019, 2020, 2021, 2022, 2023], 
    "nordic_countries" : {}, # nothing to exclude
    "save_dataset" : 0, # no intermediate df to save
    "reduce_fire_points" : 50, #sampling of fires
    "gridsearch" : 0,
    "ntree" : 100,
    "max_depth" : 15,
    "drop_neg_and_na_annual": 0, # we dont process negative values, they are present in the spi
    "name_col_y_fires" : "date_iso",
    "make_CV" : 0,
    "make_plots" : 0, 
    # settings for validation - skipped    
    "validation_ba_dict" :   {
                                "fires10" : "",
                                "fires90" : ""
                                    },
    "country_name" : "", 
    "pixel_size" : 100, 
    "user_email" : "",
    "email_pwd" : "" 
}  



#%% train the model

# set working dir and initialize the class
dir_model_name = 'model'
working_dir = f'{DATAPATH}/{dir_model_name}/{VS}'
os.makedirs(working_dir, exist_ok=True)

# defines the input for dataset creation and model training
tiles = os.listdir(TILES_DIR)
tiles = [tile for tile in tiles if os.path.isdir(os.path.join(TILES_DIR, tile))]
# eclude tile_7 because it has no fires
tiles = [tile for tile in tiles if tile not in ['tile_7']]

# tiles = ['tile_7'] # os.listdir(TILES_DIR) # tiles to clip the data on  # 

print(len(tiles))

# create a df indipendently per tile 
def create_partial_df(tile):
    
    # create a different instance for each tile
    instance_working_dir = f'{working_dir}/{tile}'
    os.makedirs(instance_working_dir, exist_ok=True)
    supranationalmodel = SupranationalModel(instance_working_dir, CONFIG)


    # dinamic variables 
    monthly_variable_names = [ 
                                # 'SPI_1m', 'SPI_3m', 'SPI_6m',
                                # 'SPEI_1m', 'SPEI_3m', 'SPEI_6m',
                                'P_1m', 'P_3m', #'P_6m',
                                # 'Tanomaly_1m', 'Tanomaly_3m', 'Tanomaly_6m',
                                'T_1m', 'T_3m', #'T_6m',            
                                ]


    years = list(range(2011, 2024)) # for training
    months = list(range(1, 13)) # all months

    # setting the structure of input dict
    # in monthly analisys the folder structur is: country --> year_month (where month is 1, 2, ..., 12, ie 2020_1) --> list of tiff file of dynimac indices
    monthly_files = {
        tile: {
            f'{year}_{month}': {
                tiffile: f'{DATAPATH}/ML/{tile}/climate_1m_shift/{year}_{month}/{tiffile}_bilinear_epsg3857.tif'
                for tiffile in monthly_variable_names
            }
            for year in years for month in months
        }
        # for tile in tiles
    }


    # static variables
    mandatory_input_dict = {tile:
                                [f"{TILES_DIR}/{tile}/dem/dem_20m_3857.tif",
                                f"{TILES_DIR}/{tile}/veg/veg_20m_3857.tif",
                                f"{TILES_DIR}/{tile}/fires/fires_2007_2023_epsg3857.shp"]
                            }
                                # for tile in tiles}

    # no other static input
    optional_input_dict = None 


    # create datasets
    try:
        X_path, Y_path = supranationalmodel.creation_dataset_annual(annual_features_paths = monthly_files, 
                                                                    mandatory_input_dict = mandatory_input_dict,
                                                                    optional_input_dict = optional_input_dict
                                                                )
    except Exception as e:
        print(f'\n\n\nERROR in {tile} \n {e} \n\n\n')
        logging.error(f'\n\n\nError in {tile} \n {e} \n\n\n')
        


with mp.Pool(len(tiles)) as pool:
    pool.map(create_partial_df, tiles)



#%% create a single df, train model and save it

import pandas as pd

# merge the x and y datasets
# tiles = os.listdir(TILES_DIR)

# at first I move the interest xls files in the main folder
for tile in tiles:
    path = f'{working_dir}/{tile}'
    if os.path.exists(path):
        xfull_path = os.path.join(path, f'X_{tile}_batchnum0.csv')
        yfull_path = os.path.join(path, f'Y_{tile}_batchnum0.csv')
        os.system(f'mv {xfull_path} {working_dir}')
        os.system(f'mv {yfull_path} {working_dir}')

df_x = []
df_y = []
for tile in tiles:
    # load the data
    X_path = f'{working_dir}/X_{tile}_batchnum0.csv'
    Y_path = f'{working_dir}/Y_{tile}_batchnum0.csv'

    # read the data
    X = pd.read_csv(X_path, index_col=0)
    Y = pd.read_csv(Y_path, index_col=0)

    # merge the data
    df_x.append(X)
    df_y.append(Y)

df_x = pd.concat(df_x)
df_y = pd.concat(df_y)
# save the data
df_x.to_csv(f'{working_dir}/X_merged.csv')
df_y.to_csv(f'{working_dir}/Y_merged.csv')

# remove coords from X and save it again
_x = pd.read_csv(f'{working_dir}/X_merged.csv', index_col=0)
_x = _x.drop(columns=['lat', 'lon'])
_x.to_csv(f'{working_dir}/X_merged_no_coords.csv')


model_path = f'{working_dir}/RF_bil_100t_15d_50samples.sav'
X_path = f'{working_dir}/X_merged.csv'
Y_path = f'{working_dir}/Y_merged.csv'
# train the model
supranationalmodel = SupranationalModel(working_dir, CONFIG)
supranationalmodel.creation_model(X_path, Y_path, model_path)


#%%
