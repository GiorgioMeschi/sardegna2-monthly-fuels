
#%%

import json 
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
import os
import rasterio as rio
from geospatial_tools import geotools as gt
from geospatial_tools import FF_tools

ft = FF_tools.FireTools()
Ras = gt.Raster()

from risico_operational.settings import HOME, DATAPATH, VS

#%%

# define function to plug into get risico point operational function. 
def plot_maps(year, month, outdir, hist_run):
    """
    Plot the fuel maps, susceptibility, dynamic and static inputs.
    """

    #drought variables
    # year, month = datetime.now().year, datetime.now().month
    if not hist_run:
        month = month - 1 # previos month wrt the actual for the avaialble data
    aggrs = [1, 3, 6]  # aggregation in months
    savepath = f'{HOME}/streamlit_view'

    # SPI
    for aggr in aggrs:

        basep = f'/mnt/drought-ita-share/archive/Italy/SPI/MCM/maps/{year}/{month:02}'

        day = os.listdir(basep)[-1]
        name = f'SPI{aggr}-MCM_{year}{month:02}{day}.tif'
        path = f'{basep}/{day}/{name}'

        outfile = f'{savepath}/{year}-{month:02}-{day}/SPI{aggr}_{year}-{month:02}-{day}.png'
        output_folder_spi = os.path.dirname(outfile)
        os.makedirs(output_folder_spi, exist_ok=True)
        
        if not os.path.exists(outfile):

            title = f'SPI{aggr} - {year}/{month:02}/{day}'

            with rio.open(path) as src:
                arr = src.read(1)  # band 1
                arr_cropped = arr[600:900, 200:500]

            fig, ax = Ras.plot_raster(arr_cropped, cmap='RdBu', title=title,
                                    dpi = 200)

            for img in ax.get_images(): 
                img.set_clim(vmin=-3, vmax=3)
            ax.figure.canvas.draw_idle()

            fig.savefig(outfile, dpi=200, bbox_inches='tight')
        

    #SPEI
    for aggr in aggrs:

        basep = f'/mnt/drought-ita-share/archive/Italy/SPEI/MCM-DROPS/maps/{year}/{month:02}'

        day = os.listdir(basep)[-1]
        name = f'SPEI{aggr}-MCM-DROPS_{year}{month:02}{day}.tif'
        path = f'{basep}/{day}/{name}'

        outfile = f'{savepath}/{year}-{month:02}-{day}/SPEI{aggr}_{year}-{month:02}-{day}.png'
        out_folder_spei = os.path.dirname(outfile)
        os.makedirs(out_folder_spei, exist_ok=True)
        
        if not os.path.exists(outfile):

            title = f'SPEI{aggr} - {year}/{month:02}/{day}'

            with rio.open(path) as src:
                arr = src.read(1)  # band 1
                arr_cropped = arr[600:900, 200:500]

            fig, ax = Ras.plot_raster(arr_cropped, cmap='RdBu', title=title)

            for img in ax.get_images(): 
                img.set_clim(vmin=-5, vmax=5)
            ax.figure.canvas.draw_idle()

            fig.savefig(outfile, dpi=300, bbox_inches='tight')
        
    # check out folders are the same
    if output_folder_spi == out_folder_spei:
        print(f"Output folders are the same: {output_folder_spi}")

    ouput_folder = output_folder_spi


    # fuel map
    hazard_file = f'{outdir}/fuel12cl_wgs84.tif'
    crs = 'EPSG:4326'
    fires_file = f'{DATAPATH}/raw/burned_area/incendi_dpc_2007_2023_sardegna_32632.shp'
    fire_col = 'date_iso'

    # readt filename in metaata.txt
    with open(f'{outdir}/metadata.txt', 'r') as f:
        meta = f.readlines()
        year = int(meta[0].strip().split('_')[1])
        month = int(meta[0].strip().split('_')[2])
        if hist_run:
            month = month + 1 # since the spi data are related to the month already in place.
    
    # year = 2025
    # month = 7

    # save csv with percentage of class 
    with rio.open(hazard_file) as haz:
        haz_arr = haz.read(1)
        haz_ndoata = haz.nodata
        unique, counts = np.unique(haz_arr, return_counts=True)
        total_pixels = np.where(haz_arr==haz_ndoata, 0, 1).sum()
        percentages = {int(k): int((v / total_pixels) * 100) for k, v in zip(unique, counts) if k != haz_ndoata}
    with open(f'{ouput_folder}/fuel_percentage.csv', 'w') as f:
        f.write('Fuel_Class,Percentage\n')
        for k, v in percentages.items():
            f.write(f'{k},{v}\n')

    settings = dict(
        fires_file=         fires_file,
        fires_col=          fire_col,
        crs=                crs,
        hazard_path=         hazard_file,
        xboxmin_hist=       0.1,
        yboxmin_hist=       0.1,
        xboxmin_pie=        0.1,
        yboxmin_pie=        0.7,
        out_folder=         ouput_folder,
        year=               year,
        month=              month,
        season=             False,
        haz_nodata=         0,
        pixel_to_ha_factor= 1,
        allow_hist=         False,
        allow_pie=          True,
        allow_fires=        False,
    )

    ft.plot_haz_with_bars(**settings)

    # susc

    susc_path = f'{DATAPATH}/susceptibility/{VS}/susc_{year}_{month}.tif'
    tr_path = f'{DATAPATH}/susceptibility/{VS}/thresholds/thresholds.json'
    thresholds = json.load(open(tr_path))
    tr1, tr2 = thresholds['lv1'], thresholds['lv2']

    # reproject the susc in epsg4326 to plot not distorted
    dem_file = f'{DATAPATH}/raw/dem/dem_ispra_100m_wgs84.tif'
    susc_path_wgs = f'{DATAPATH}/susceptibility/{VS}/susc_{year}_{month}_wgs84.tif'
    if os.path.exists(susc_path_wgs):
        os.remove(susc_path_wgs)
    Ras.reproject_raster_as_v2(in_file=susc_path,
                            out_file=susc_path_wgs,
                                reference_file=dem_file, 
                                input_crs = 'EPSG:32632', working_crs = 'EPSG:4326', interpolation = 'near')


    settings = dict(
        fires_file= fires_file,
        fires_col= 'date_iso', # 'finaldate',
        crs= 'epsg:4326',
        susc_path= susc_path_wgs,
        xboxmin_hist= 0.1,
        yboxmin_hist= 0.1,
        xboxmin_pie= 0.1,
        yboxmin_pie= 0.7,
        threshold1= tr1,
        threshold2= tr2,
        out_folder= ouput_folder,
        year= year,
        month= month,
        season= False,
        total_ba_period= 1,
        susc_nodata= -1,
        pixel_to_ha_factor= 1,
        allow_hist= False,
        allow_pie= True,
        allow_fires= False,
        normalize_over_y_axis= 10,
        limit_barperc_to_show= 1,
    )

    ft.plot_susc_with_bars(**settings)

    #dem slope northing easting
    slope_file = f'{DATAPATH}/raw/dem/slope_100m_wgs84.tif'
    aspect_file = f'{DATAPATH}/raw/dem/aspect_100m_wgs84.tif'
    veg_file = f'{DATAPATH}/raw/vegetation/vegetation_3dig_wgs.tif'
    static_outfolder = f'{savepath}/static'
    os.makedirs(static_outfolder, exist_ok=True)

    for file, title in zip([dem_file, slope_file, aspect_file], ['DEM', 'Slope', 'Aspect']):

        filename = f'{static_outfolder}/{title}.png'
        if not os.path.exists(filename):
            with rio.open(file) as src:
                arr = src.read(1)  # band 1
                # remove 0
                arr[arr <= 0] = np.nan

                Ras.plot_raster(arr,
                                cmap='terrain',
                                title=title,
                                shrink_legend=0.6,
                                outpath=filename,
                                # figsize=(12, 10),
                                dpi = 200)


    # vegatation with discrete palette using matplotlib
    filename = f'{static_outfolder}/vegetation.png'

    if not os.path.exists(filename):

        with rio.open(veg_file) as src:
            arr = src.read(1)

        values = [-100000,  0.1, 111, 112, 121, 122, 123, 124, 131, 132, 133, 141, 142, 211,
            212, 213, 221, 222, 223, 224, 231, 241, 242, 243, 244, 311, 312,
            313, 321, 322, 323, 324, 331, 332, 333, 334, 411, 421, 422, 
            511, 512, 521, 522, 523, 10000]

        values_offset = [v + 0.5 for v in values]  # offset to center the values in the color bins
        color_dict = {
            "no data": "#ffffff",   # from negative and 0
            "111": "#d4d4d4",
            "112": "#d4d4d4",
            "121": "#c2c2c2",
            "122": "#b0b0b0",
            "123": "#9e9e9e",
            "124": "#8c8c8c",
            "131": "#7a7a7a",
            "132": "#686868",
            "133": "#565656",
            "141": "#444444",
            "142": "#323232",
            "211": "#fff0b3",
            "212": "#ffe680",
            "213": "#ffdb4d",
            "221": "#ffd11a",
            "222": "#e6c200",
            "223": "#cca300",
            "224": "#b38f00",
            "231": "#998000",
            "241": "#806600",
            "242": "#665200",
            "243": "#4d3d00",
            "244": "#4d3d00",
            "311": "#e23b3b",
            "312": "#ce09dc",
            "313": "#1aa31a",
            "321": "#008f00",
            "322": "#007300",
            "323": "#005c00",
            "324": "#004700",
            "331": "#003300",
            "332": "#002200",
            "333": "#002200",
            "334": "#002200",
            "411": "#ffa64d",
            "421": "#b97d3e",
            "422": "#e17a0b",
            "511": "#ffffff",
            "512": "#ffffff",
            "521": "#ffffff",
            "522": "#ffffff",
            "523": "#ffffff",
            "-": "#ffffff"
            }

        Ras.plot_raster(arr,
                        array_classes = values_offset, array_colors = list(color_dict.values()), array_names = list(color_dict.keys()),
                        title='Vegetation',
                        shrink_legend=0.8,
                        labelsize = 6.5,
                        outpath = f'{static_outfolder}/vegetation.png',
                        # figsize=(12, 10),
                        dpi = 200)
        

    # return the list of output images
    plt.close('all')

    return ouput_folder, static_outfolder
                
    

#%% 

# dyn_output_folder, static_output_folder = plot_maps()

# name = 'sardegna-medstar'
# destination_repo = f"{HOME}/viewer/ml-viewer"
# destination_subfolder = os.path.join(destination_repo, "data", name) 

# os.system(f"cp -r {dyn_output_folder} {destination_subfolder}")
# os.system(f"cp -r {static_output_folder} {destination_subfolder}")


# # === 2. Git add, commit, and push using os.system ===
# os.chdir(destination_repo)

# os.system("git remote set-url origin https://github.com/GiorgioMeschi/ml-viewer.git")

# commit_message = "Auto update: add new data files"
# os.system("git add .")
# os.system(f'git commit -m "{commit_message}"')
# import subprocess
# try:
#     subprocess.run(["git", "push", "origin", "main"], check=True)
# except subprocess.CalledProcessError as e:
#     print("❌ Git push failed:", e)
# os.system("git push origin main")

#%%
