

#%% TRAINING

from risico_operational.settings import DATAPATH
from annual_wildfire_susceptibility.supranational_model import SupranationalModel

CONFIG = {     
    "batches" : 1, 
    "nb_codes_list" : [1],
    "list_features_to_remove" : ["lat", "lon"],
    "convert_to_month" : 0, # we turn on here here the month setting 
    "aggr_seasonal": 0, # we turn off the seasonal analysis.
    "wildfire_years" : [2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023], 
    "nordic_countries" : {}, # nothing to exclude
    "save_dataset" : 0, # no intermediate df to save
    "reduce_fire_points" : 50, #sampling of fires
    "gridsearch" : 0,
    "ntree" : 100,
    "max_depth" : 15,
    "drop_neg_and_na_annual": 0, # we dont process negative values, they are present in the SPI
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


# set working dir and initialize the class
dir_model_name = 'static'
working_dir = f'{DATAPATH}/model/{dir_model_name}'
os.makedirs(working_dir, exist_ok=True)

supranationalmodel = SupranationalModel(working_dir, CONFIG)

# defines the input for dataset creation and model training
countries = ['sardegna2'] # countries to be included in the model

# create a dictionary with dem, veg and fires, they mandatory for dataset creation, keep this order
mandatory_input_dict = {country:
                            [f"{DATAPATH}/raw/dem/dem_ispra_100m_32632_v2.tif",
                            f"{DATAPATH}/raw/vegetation/vegetation_ml_32632.tif",
                            f"{DATAPATH}/raw/burned_area/incendi_dpc_2007_2023_sardegna_32632.shp"]
                            for country in countries}


optional_input_dict = None 

X_path, Y_path = supranationalmodel.creation_dataset_static(
                                                            mandatory_input_dict = mandatory_input_dict,
                                                            optional_input_dict = optional_input_dict
                                                        )

# create a model and save it
model_path = f'{working_dir}/RF_100t_15d_50samples.sav'
supranationalmodel.creation_model(X_path, Y_path, model_path)



#%% RUN

import os
import pandas as pd

from annual_wildfire_susceptibility.susceptibility import Susceptibility

# directory with output reuslts:
working_dir = f'{DATAPATH}/susceptibility/{dir_model_name}'
os.makedirs(working_dir, exist_ok=True)
country = 'sardegna2'

# provide in input dem, veg and an optional layer
dem_path = f"{DATAPATH}/raw/dem/dem_ispra_100m_32632_v2.tif"
veg_path = f"{DATAPATH}/raw/vegetation/vegetation_ml_32632.tif"
optional_input_dict = {}


# initialize che class
CONFIG.update({'list_features_to_remove': [],
               })
susceptibility = Susceptibility(dem_path, veg_path, # mandatory vars
                                working_dir = working_dir,
                                optional_input_dict = optional_input_dict, # optional layers
                                config = CONFIG # configuration file
                                ) 

# run the model passing the dataset used for training as reference
model_path = f'{DATAPATH}/model/{dir_model_name}/RF_100t_15d_50samples.sav' # this is found in the out folder in the phase of model training                             
X_path = f'{DATAPATH}/model/{dir_model_name}/X_no_coords.csv'

if not os.path.exists(X_path):
    f = pd.read_csv(f'{DATAPATH}/model/{dir_model_name}/X.csv', index_col=0)
    # drop lat lon
    f = f.drop(columns=['lat', 'lon'])
    # save the file
    f.to_csv(X_path)

susceptibility.run_existed_model(model_path, 
                                training_df_path = X_path)



# %%



