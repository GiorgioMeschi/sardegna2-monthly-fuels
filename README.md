# calabria2-monthly-fuels
operational chain and historical analyses on calabria at 20m resolution to produce monthly fuel maps using MCM drought data at 1km resolution 

# env installation
strongly suggested environment preparation:

conda create --prefix .venv/ python=3.12.1 gdal

conda activate .venv/

pip install ipykernel

python -m pip install --no-cache-dir -U git+https://github.com/GiorgioMeschi/Annual_Wildfire_Susceptibility.git

python -m pip install --no-cache-dir -U git+https://github.com/GiorgioMeschi/geospatial_tools

# versions

v1 --> including P,T SPI and SPEI 1 3 6 months using Annual_Wildfire_Susceptibility v1
v2 --> excluding P and T because highly correlated with fire seasonality = less capability to discriminate the months in the same season, and sharp distinction between winter and summer seasons. 
Annual_Wildfire_Susceptibility v2 is used, this version samples a defined percentage of fires in all the months
while retaining only pseudo absences from the months below the average fire activity in the analyzed period. 
50% of samples for each month.
v3 --> using only P and T and Annual_Wildfire_Susceptibility v2, becasue spi and spei might not reflet the real drought along the year, but they compare only the same months in different years. (i.e Aug and Dec can have the same value, but very different fire behaviour becasue of potential different absolute value.)
