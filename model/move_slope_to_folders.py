

'''
move the slope already created from each tile to each tile folder to avoind the gdal bug when generating the slope
'''

from risico_operational.settings import TILES_DIR
import os

tiles = os.listdir(TILES_DIR)
vs = 'v1'

for tile in tiles:
    print(tile)
    input_folder = f'/share/drought/projects/sardegna2/data/model/{vs}'
    input_path = os.path.join(input_folder, tile, 'aspect.tif')
    out_path = os.path.join(TILES_DIR, tile, 'susceptibility', vs, 'aspect.tif')
    # copy
    os.system(f'cp {input_path} {out_path}')

