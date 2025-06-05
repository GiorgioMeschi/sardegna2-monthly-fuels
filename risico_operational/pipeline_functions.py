

import os
import numpy as np
from geospatial_tools import FF_tools as ff
from geospatial_tools import geotools as gt
import rasterio as rio
from rasterio.merge import merge
import geopandas as gpd 
from rasterio.mask import mask as riomask
import time
import json
import subprocess
# import boto3
import logging 

from risico_operational.settings import TILES_DIR


 

def clip_to_tiles(var, aggr, year: str, month: str, tile: str, 
                  tile_df: gpd.GeoDataFrame, current_year: str, current_month: str):

    '''
    clip data on server drought to calabria and resample it to 20m resolution.
    it works for SPI and SPEI vars.
    '''

    # folderpath changes depending on the variables
    if var == 'SPI':
        basep = f'/home/drought/drought_share/archive/Italy/{var}/MCM/maps/{year}/{month:02}'
        day = os.listdir(basep)[-1]
        name = f'{var}{aggr}-MCM_{year}{month:02}{day}.tif'
        path = f'{basep}/{day}/{name}'

    elif var == 'SPEI':
        basep = f'/home/drought/drought_share/archive/Italy/{var}/MCM-DROPS/maps/{year}/{month:02}'
        day = os.listdir(basep)[-1]
        name = f'{var}{aggr}-MCM-DROPS_{year}{month:02}{day}.tif'
        path = f'{basep}/{day}/{name}'

    out_folder = os.path.join(TILES_DIR, tile, 'climate', f'{current_year}_{current_month}')
    os.makedirs(out_folder, exist_ok=True)
    wgs_file = os.path.join(out_folder, f'{var}_{aggr}m_orig.tif')
    reproj_out_file = os.path.join(out_folder, f'{var}_{aggr}m_bilinear_epsg3857.tif') # out filename

    # clip and reproject
    tile_geom = tile_df[tile_df['id_sorted'] == int(tile[5:])].geometry.values[0]
    # buffer 5 km in degrees
    tile_geom = tile_geom.buffer(0.05)
    
    with rio.open(path) as src:
        out_image, out_transform = riomask(src, [tile_geom], crop = True)
        out_meta = src.meta.copy()
        out_meta.update({
            'height': out_image.shape[1],
            'width': out_image.shape[2],
            'transform': out_transform
        })
        with rio.open(wgs_file, 'w', **out_meta) as dst:
            dst.write(out_image)
    
    reference_file = os.path.join(TILES_DIR, tile, 'dem', 'dem_20m_3857.tif')
    with rio.open(reference_file) as ref:
        bounds = ref.bounds  # Extract bounds (left, bottom, right, top)
        xres = ref.transform[0]  # Pixel width
        yres = ref.transform[4]  # Pixel height
        working_crs = 'EPSG:3857'  # Target CRS

    # Use gdalwarp to reproject and match the bounds and resolution of the reference file
    interpolation = 'bilinear'  # Interpolation method
    os.system(
        f'gdalwarp -t_srs {working_crs} -te {bounds.left} {bounds.bottom} {bounds.right} {bounds.top} '
        f'-tr {xres} {yres} -r {interpolation} -of GTiff '
        f'-co COMPRESS=LZW -co TILED=YES -co BLOCKXSIZE=256 -co BLOCKYSIZE=256 '
        f'{wgs_file} {reproj_out_file}'
    )

    time.sleep(2)
    os.remove(wgs_file)


def merge_susc_tiles(tiles, year, month, outfolder):

    vs = 'v2'
    files_to_merge = [f"{TILES_DIR}/{tile}/susceptibility/{vs}/{year}_{month}/susceptibility/annual_maps/Annual_susc_{year}_{month}.tif"
                    for tile in tiles]

    outfile = os.path.join(outfolder, f'susc_calabria_{year}_{month}.tif')
    # out = ras.merge_rasters(outfile, np.nan, 'first', *files_to_merge)
    ras = [rio.open(i) for i in files_to_merge]
    arr, trans = merge(ras, nodata = np.nan, method = 'first')
    arr[np.isnan(arr)] = -1
    with rio.open(files_to_merge[0]) as src:
        out_meta = src.meta.copy()
        out_meta.update({
            'compress': 'lzw',
            'tiled': True,
            'blockxsize': 256,
            'blockysize': 256,
            'height': arr.shape[1],
            'width': arr.shape[2],
            'transform': trans,
        })
        with rio.open(outfile, 'w', **out_meta) as dst:
            dst.write(arr)
    
    return outfile


def generate_fuel_map(merged_susc_file, threshold_file, veg_path, mapping_path, out_file):

    fft = ff.FireTools()

    thresholds = json.load(open(threshold_file))
    tr1, tr2 = thresholds['lv1'], thresholds['lv2']

    inputs = dict(
        susc_path = merged_susc_file,
        thresholds= [tr1, tr2],
        veg_path = veg_path,
        mapping_path = mapping_path,
        out_hazard_file = out_file
        )

    fft.hazard_12cl_assesment(**inputs) #save fuel map file on 'out_file' location
    

def reproject_raster_as(in_file, out_file, reference_file, input_crs = 'EPSG:3857', working_crs = 'EPSG:4326'):

    with rio.open(reference_file) as ref:
        bounds = ref.bounds
        xres = ref.transform[0]  # Pixel width
        yres = ref.transform[4]  # Pixel height
        interpolation = 'near'

    # forse input output crs to avoind gdal internal errors
    os.system(
        f'gdalwarp -s_srs {input_crs} -t_srs {working_crs} -te {bounds.left} {bounds.bottom} {bounds.right} {bounds.top} '
        f'-tr {xres} {yres} -r {interpolation} -of GTiff '
        f'-co COMPRESS=LZW -co TILED=YES -co BLOCKXSIZE=256 -co BLOCKYSIZE=256 '
        f'{in_file} {out_file}'
    )


def write_risico_files(fuel12cl_path, slope_path, aspect_path, outfile):
    # logging.info(f'read {fuel12cl_path}')
    with rio.open(fuel12cl_path, 'r') as src:
        values = src.read(1)

        # Get the geographic coordinate transform (affine transform)
        transform = src.transform
        # Generate arrays of row and column indices

    # logging.info('Generate arrays of row and column indices')
    rows, cols = np.indices((src.height, src.width))        
    # mask rows and cols to get only the valid pixels (where hazard not 0)
    rows = rows[values != 0]
    cols = cols[values != 0]
    # Transform pixel coordinates to geographic coordinates
    lon, lat = transform * (cols, rows)
    # logging.info('Extract values')
    hazard = values[rows, cols]
    

    # get value of raster in those coordinates
    with rio.open(slope_path) as src:
        values = src.read(1)
        slope = values[rows, cols]
    
    with rio.open(aspect_path) as src:
        values = src.read(1)
        aspect = values[rows, cols] 
        
                
    with open(outfile, 'w') as ff:
        # write header
        ff.write('# lon lat slope aspect veg_id \n')
        for i in range(len(hazard)):
            veg_id = hazard[i]
            if veg_id < 0:
                veg_id = 0
            new_line = f'{lon[i]:.5f} {lat[i]:.5f} {slope[i]:.2f} {aspect[i]:.2f} {veg_id}'
            ff.write(f'{new_line}\n')    