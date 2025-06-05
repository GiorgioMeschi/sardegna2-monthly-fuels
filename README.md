# sardegna2-monthly-fuels
operational chain and historical analyses at 100m resolution to produce monthly fuel maps using MCM drought - data at 1km resolution 

# env installation
strongly suggested environment preparation:

conda create --prefix .venv/ python=3.12.1 gdal

conda activate .venv/

pip install ipykernel

python -m pip install --no-cache-dir -U git+https://github.com/GiorgioMeschi/Annual_Wildfire_Susceptibility.git

python -m pip install --no-cache-dir -U git+https://github.com/GiorgioMeschi/geospatial_tools

pip install numpy=1.26.0

