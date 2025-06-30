
#%%
import os
import json
import numpy as np
from geospatial_tools import FF_tools as ff
from geospatial_tools import geotools as gt

from home import DATAPATH

fft = ff.FireTools()
Raster = gt.Raster()

#%%

vs_susc = 'v1'
folder_susc = f'{DATAPATH}/susceptibility/{vs_susc}'
susc_names = [i for i in os.listdir(folder_susc) if i.endswith('.tif')]
threashold_file = f'{DATAPATH}/susceptibility/{vs_susc}/thresholds/thresholds.json'
thresholds = json.load(open(threashold_file))
tr1, tr2 = thresholds['lv1'], thresholds['lv2']
veg_path = f'{DATAPATH}/raw/vegetation/vegetation_3dig_32632.tif'
mapping_path = f'{DATAPATH}/raw/vegetation/veg_to_ft2.json' # already the input is with aggregation
out_folder = f'{DATAPATH}/fuel_maps/{vs_susc}'
os.makedirs(out_folder, exist_ok=True)
susc_class_oufolder = f'{DATAPATH}/susceptibility/{vs_susc}/susc_classified'
ft_outfolder = f'{DATAPATH}/fuel_type_4cl/{vs_susc}'
os.makedirs(susc_class_oufolder, exist_ok=True)
os.makedirs(ft_outfolder, exist_ok=True)

# get hazards
def hazard(susc_filename):
    
    susc_file = f"{folder_susc}/{susc_filename}"
    hazard_filename = susc_filename.replace('susc', 'fuel')

    inputs = dict(
        susc_path = susc_file,
        thresholds= [tr1, tr2],
        veg_path = veg_path,
        mapping_path = mapping_path,
        out_hazard_file = f"{out_folder}/{hazard_filename}"
        )

    _, susc_class, ft_arr = fft.hazard_12cl_assesment(**inputs)
    # save
    Raster.save_raster_as(susc_class, 
                          f'{susc_class_oufolder}/{susc_filename}',
                          susc_file, dtype = np.int8(), nodata =0)
    
    Raster.save_raster_as(susc_class, 
                          f'{susc_class_oufolder}/{susc_filename}',
                          susc_file, dtype = np.int8(), nodata =0)
    
    ft_filename = 'ft.tif'
    if not os.path.exists(f'{ft_outfolder}/{ft_filename}'):
        Raster.save_raster_as(ft_arr,
                                f'{ft_outfolder}/{ft_filename}',
                                susc_file, dtype = np.int8(), nodata =0)

import multiprocessing as mp
with mp.Pool(processes=20) as pool:
    pool.map(hazard, susc_names)

    

#%% get the static fuel map

vs_susc = 'static'

susc_file = f'{DATAPATH}/susceptibility/{vs_susc}/susceptibility/SUSCEPTIBILITY.tif'
threashold_file = f'{DATAPATH}/susceptibility/{vs_susc}/thresholds/thresholds.json'
thresholds = json.load(open(threashold_file))
tr1, tr2 = thresholds['lv1'], thresholds['lv2']
veg_path = f'{DATAPATH}/raw/vegetation/vegetation_3dig_32632.tif'
mapping_path = f'{DATAPATH}/raw/vegetation/veg_to_ft2.json' # already the input is with aggregation
out_folder = f'{DATAPATH}/fuel_maps/{vs_susc}'
os.makedirs(out_folder, exist_ok=True)
susc_class_oufolder = f'{DATAPATH}/susceptibility/{vs_susc}/susc_classified'
ft_outfolder = f'{DATAPATH}/fuel_type_4cl/{vs_susc}'
os.makedirs(susc_class_oufolder, exist_ok=True)
os.makedirs(ft_outfolder, exist_ok=True)



hazard_filename = 'STATIC_FUEL_MAP.tif'
inputs = dict(
    susc_path = susc_file,
    thresholds= [tr1, tr2],
    veg_path = veg_path,
    mapping_path = mapping_path,
    out_hazard_file = f"{out_folder}/{hazard_filename}"
    )

_, susc_class, ft_arr = fft.hazard_12cl_assesment(**inputs)
# save
susc_filename = 'susc_3classes.tif'
Raster.save_raster_as(susc_class, 
                        f'{susc_class_oufolder}/{susc_filename}',
                        susc_file, dtype = np.int8(), nodata =0)

ft_filename = 'ft.tif'
Raster.save_raster_as(ft_arr,
                        f'{ft_outfolder}/{ft_filename}',
                        susc_file, dtype = np.int8(), nodata =0)



# %%

