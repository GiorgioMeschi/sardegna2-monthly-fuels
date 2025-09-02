
#%%
'''
implement logic of fuel map creation if new spi/spei are available
select the tile covering italy and clip + reproject over tiles
generate susceptibility per tile using the library
merge the suscpetibilities 
generate the fuel map 
generate 1 time only aspect and slope for risico txt
add to risico txt the fuel type 
'''

#%%

import os
import sys
import multiprocessing as mp
import geopandas as gpd
from datetime import datetime as dt
from datetime import timedelta
import logging
import pyproj

try: 
    from osgeo import gdal
except Exception as e:
    logging.info(e)
    logging.info('trying importing gdal direclty')
    import gdal

# chdir to filename repo directory to fix all the imports
f = os.path.dirname(
            os.path.dirname(os.path.abspath(__file__)
                            ))
                            
sys.path.append(f)
os.chdir(f)
print(f)

from risico_operational.settings import HOME, DATAPATH, TILES_DIR, VS
from model.run_model import compute_susceptibility
from risico_operational.pipeline_functions import (get_spei1_rawfile, get_spei3_rawfile, get_spei6_rawfile,
                                                    get_spi6_rawfile, get_spi1_rawfile, get_spi3_rawfile,
                                                    find_latest,
                                                    clip_to_tiles, merge_susc_tiles, generate_fuel_map,
                                                    write_risico_files, reproject_raster_as, remove_borders)

from risico_operational.views.plot_maps.plot import plot_maps


#%%

AGGREGATIONS = [1, 3, 6] # month of aggregation for 1 index 
AOI_PATH = f'{DATAPATH}/aoi'
MERGED_SUSC_DIR = f'{DATAPATH}/susceptibility/{VS}' # path to save the merged susceptibility maps
VEG_DIR = f'{DATAPATH}/raw/vegetation' # path to the vegetation map
SUSC_THRESHOLD_DIR = f'{MERGED_SUSC_DIR}/thresholds' # path to the thresholds
TOPOGRAPHIC_DATA = f'{DATAPATH}/raw/dem' # path to the topographic data
OUTPUT_DIR = f'{DATAPATH}/risico' # path to save the output files

# if true go and change manually the current date of your choice and it will produce fuel map and risico points for that month
HISTORICAL_RUN = False 

pyproj_path = pyproj.datadir.get_data_dir()
os.environ["GTIFF_SRS_SOURCE"] = "EPSG"
os.environ["PROJ_DATA"] = pyproj_path


def pipeline(date):

    year = date.year
    month = date.month
    logging.info(f'Running for date: {date}')
    print(os.getcwd()) 

    try:
        spi1_rawpath, found_date = find_latest(get_spi1_rawfile, date)
        spi1_actualmonth = found_date.strftime('%m')
        spi1_actualyear = found_date.strftime('%Y')
    except ValueError:
        raise Exception("Could not find data for spi1 in the latest 90 days")
    
    try:
        spi3_rawpath, found_date = find_latest(get_spi3_rawfile, date)
        spi3_actualmonth = found_date.strftime('%m')
        spi3_actualyear = found_date.strftime('%Y')
    except ValueError:        
        raise Exception("Could not find data for spi3 in the latest 90 days")
    
    try:
        spi6_rawpath, found_date = find_latest(get_spi6_rawfile, date)
        spi6_actualmonth = found_date.strftime('%m')
        spi6_actualyear = found_date.strftime('%Y')
    except ValueError:        
        raise Exception("Could not find data for spi6 in the latest 90 days")

    try:
        spei1_rawpath, found_date = find_latest(get_spei1_rawfile, date)
        spei1_actualmonth = found_date.strftime('%m')
        spei1_actualyear = found_date.strftime('%Y')
    except ValueError:
        raise Exception("Could not find data for spei1 in the latest 90 days")
    
    try:
        spei3_rawpath, found_date = find_latest(get_spei3_rawfile, date)
        spei3_actualmonth = found_date.strftime('%m')
        spei3_actualyear = found_date.strftime('%Y')
    except ValueError:        
        raise Exception("Could not find data for spei3 in the latest 90 days")
    
    try:
        spei6_rawpath, found_date = find_latest(get_spei6_rawfile, date)
        spei6_actualmonth = found_date.strftime('%m')
        spei6_actualyear = found_date.strftime('%Y')
    except ValueError:        
        raise Exception("Could not find data for spei6 in the latest 90 days")

    logging.info(f'''
    found 
        SPI1: {spi1_rawpath}
        SPI3: {spi3_rawpath}
        SPI6: {spi6_rawpath}
        SPEI1: {spei1_rawpath}
        SPEI3: {spei3_rawpath}
        SPEI6: {spei6_rawpath}
    ''')

    # if fuel map not already computed with found indeces do it
    out_monthly_fuel_folder = f'{OUTPUT_DIR}/monthly_fuel_maps'
    os.makedirs(out_monthly_fuel_folder, exist_ok=True)
    fuel_filename = (
        f'fuel12cl_{year}_{month}_'
        f'spi1-{spi1_actualmonth}_'
        f'spi3-{spi3_actualmonth}_'
        f'spi6-{spi6_actualmonth}_'
        f'spei1-{spei1_actualmonth}_'
        f'spei3-{spei3_actualmonth}_'
        f'spei6-{spei6_actualmonth}.tif'  
         )
    
    fuel12cl_path = f'{out_monthly_fuel_folder}/{fuel_filename}'

    # write a txt with the fuel nam
    fuel12cl_txt = f'{OUTPUT_DIR}/metadata.txt'
    if os.path.exists(fuel12cl_txt):
        os.remove(fuel12cl_txt)
    with open(fuel12cl_txt, 'w') as f:
        f.write(f'{fuel_filename}\n')
    
    # generate susc, fuel and risico points
    if not os.path.isfile(fuel12cl_path):
  
        tiles = os.listdir(TILES_DIR)
        tile_df = gpd.read_file(f'{AOI_PATH}/grid_wgs_clean.geojsonl.json', driver='GeoJSONSeq')
        tile_df_wgs = tile_df.to_crs('EPSG:4326') #proj of the input SPI

        # get the current months for spi and spei data on different aggregations
        logging.info(f'preparing droung data per tile')
        valid_months = [spi1_actualmonth, spi3_actualmonth, spi6_actualmonth,
                        spei1_actualmonth, spei3_actualmonth, spei6_actualmonth]
        valid_years = [spi1_actualyear, spi3_actualyear, spi6_actualyear, 
                        spei1_actualyear, spei3_actualyear, spei6_actualyear]
        all_aggrs = AGGREGATIONS*2  # spi 1 3 6 plus spei 1 3 6
        all_vars = ['SPI']*3 + ['SPEI']*3  # 3 SPI and 3 SPEI
        pairs = [(_var, _aggr, _year, _month) for _var, _aggr, _year, _month in zip(all_vars, all_aggrs, valid_years, valid_months)]
        for _var, _aggr, _year, _month in pairs:
            with mp.Pool(30) as p:
                p.starmap(clip_to_tiles, [(_var, _aggr, _year, _month, tile, tile_df_wgs, year, month)
                                                                                for tile in tiles
                                                                                ])

            
        # get susceptibility map - includes already dynamic multiprocessing in the func
        logging.info(f'\nstart computiong susceptibility per tile\n')
        compute_susceptibility(years = [year], months = [month])

        # remove pixel border that was previouly buffered to avoid no issues at edges due to reprojections
        remove_borders(tiles, year, month) 

        # Merge susceptibility maps
        logging.info(f'\nget susceptibility\n')
        merged_susc_file = merge_susc_tiles(tiles, year, month, MERGED_SUSC_DIR)

        # reproj as dem mantaining the same name
        dem_path = f'{TOPOGRAPHIC_DATA}/dem_ispra_100m_32632_v2.tif'
        os.rename(merged_susc_file, merged_susc_file.replace('.tif', '_notfixed.tif'))
        merged_susc_file_notfixed = merged_susc_file.replace('.tif', '_notfixed.tif')
        reproject_raster_as(merged_susc_file_notfixed, merged_susc_file, dem_path,
                            input_crs='EPSG:32632', working_crs='EPSG:32632')
        os.remove(merged_susc_file_notfixed)


        # Generate fuel maps
        logging.info(f'\nget fuel map\n')
        veg_path = f'{VEG_DIR}/vegetation_3dig_32632.tif'
        mapping_path = f'{VEG_DIR}/veg_to_ft2.json'
        threashold_file = f'{SUSC_THRESHOLD_DIR}/thresholds.json'
        generate_fuel_map(merged_susc_file, threashold_file, veg_path, mapping_path, 
                           out_file = fuel12cl_path)

        #input/output for risico.txt
        dem_wgs_path = f'{TOPOGRAPHIC_DATA}/dem_ispra_100m_wgs84.tif'
        slope_wgs_path = f'{TOPOGRAPHIC_DATA}/slope_100m_wgs84.tif'
        aspect_wgs_path = f'{TOPOGRAPHIC_DATA}/aspect_100m_wgs84.tif'
        _slope_path = f'{DATAPATH}/susceptibility/static/susceptibility/slope.tif'
        _aspect_path = f'{DATAPATH}/susceptibility/static/susceptibility/aspect.tif'
        if not os.path.exists(slope_wgs_path): # if slope and aspect already exist dont do it
            logging.info(f'reproj slope and aspect')
            reproject_raster_as(_slope_path, slope_wgs_path, dem_wgs_path)
            reproject_raster_as(_aspect_path, aspect_wgs_path, dem_wgs_path)

        # reproject fuel map to wgs84
        fuel12_wgs_path = f'{OUTPUT_DIR}/fuel12cl_wgs84.tif'
        if os.path.exists(fuel12_wgs_path):
            os.remove(fuel12_wgs_path)
        reproject_raster_as(fuel12cl_path, fuel12_wgs_path, dem_wgs_path)
        logging.info(f'Fuel12cl reprojected to {fuel12_wgs_path}')

        # create a txt file in which each row has x and y coordinates and the value of the hazard
        risico_outfile = f'{OUTPUT_DIR}/risico.txt'
        logging.info(f'Write risico file to {risico_outfile}')
        if os.path.exists(risico_outfile):
            os.remove(risico_outfile)

        write_risico_files(fuel12_wgs_path, slope_wgs_path, aspect_wgs_path, risico_outfile)
        logging.info(f'{risico_outfile} created!!!')    

    
        # generate png and move to viewer
        dyn_output_folder, static_output_folder = plot_maps()
        name = 'sardegna-medstar'
        destination_repo = f"{HOME}/viewer/ml-viewer"
        destination_subfolder = os.path.join(destination_repo, "data", name) 
        os.system(f"cp -r {dyn_output_folder} {destination_subfolder}")
        os.system(f"cp -r {static_output_folder} {destination_subfolder}")
        os.chdir(destination_repo)
        os.system("git remote set-url origin https://github.com/GiorgioMeschi/ml-viewer.git")
        commit_message = "Auto update: add new data files"
        os.system("git add .")
        os.system(f'git commit -m "{commit_message}"')
        import subprocess
        try:
            subprocess.run(["git", "push", "origin", "main"], check=True)
        except subprocess.CalledProcessError as e:
            print("❌ Git push failed:", e)
       



if __name__ == '__main__':

    # get the current date
    days = [0]  # days to go back for historical runs
    for prev_day in days:
        date = dt.now() - timedelta(days=prev_day)  

        if HISTORICAL_RUN:
            date_str = date.strftime('%Y%m%d')
            # if historical run, use the first day of the month
            OUTPUT_DIR = f'{OUTPUT_DIR}/RUN_HIST_{date_str}'

        log_filename = f'{OUTPUT_DIR}/logs/pipeline_{date.strftime('%Y-%m-%d')}.log'
        os.makedirs(os.path.dirname(log_filename), exist_ok=True)
        logging.basicConfig(level=logging.INFO,
                            format = '[%(asctime)s] %(filename)s: {%(lineno)d} %(levelname)s - %(message)s',
                            datefmt ='%H:%M:%S',
                            filename = log_filename)
        
        pipeline(date)

# # read risico file lines
# with open(f'{OUTPUT_DIR}/risico.txt', 'r') as f:
#     lines = f.readlines()
#     for line in lines[200:220]:  # print first 10 lines
#         print(line.strip())



# ras.save_raster_as(rio.open(dem_wgs_path).read(1), dem_wgs_path.replace('.tif', '_fixed.tif'), dem_wgs_path, dtype = 'float32')