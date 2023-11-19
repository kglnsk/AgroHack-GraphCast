from fastapi import FastAPI
from pydantic import BaseModel
from datetime import datetime
import math
import numpy as np
import json
from datetime import datetime, timedelta
import xarray as xr
import pandas as pd
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


ds = xr.open_dataset('download.nc')

def calculate_maximum_water_pressure(Ta):
    return 611.21 * np.exp(17.502 * Ta / (240.97 + Ta))

def calculate_actual_water_pressure(Td):
    return 611.21 * np.exp(17.502 * Td / (240.97 + Td))

def calculate_relative_humidity(Td, Ta):
    max_water_pressure = calculate_maximum_water_pressure(Ta)
    actual_water_pressure = calculate_actual_water_pressure(Td)
    return 100*actual_water_pressure / max_water_pressure


# Define the disease conditions in a DataFrame
disease_conditions = pd.DataFrame({
    'Disease': ['Downy Mildew', 'Powdery Mildew', 'Anthracnose', 'Gray Mold (Botrytis cinerea)', 'Black Spot', 'Black Rot', 'White Rot', 'Verticillium Wilt', 'Alternaria Leaf Spot', 'Fusarium Wilt', 'Red Blotch (Esca)', 'Bacterial Cancer'],
    'Codename':['mild','oidium','anthra','gray','blackDots','black','white','vilt','alternarioz','fuzarioz','krasnuha','bakterial'],
    'Temperature Onset': [11, 5, (10, 15), 12, 15, '-', 14, '-', (11, 15), 1, 11, '-'],
    'Humidity Onset': [(85, 100), (60, 80), (70, 80), (95, 100), (80, 90), '-', (90, 100), '-', (80, 90), (40, 80), (80, 90), '-'],
    'Optimal Temperature': [(21, 25), (20, 35), (24, 30), (25, 30), (18, 20), (20, 25), (20, 27), (21, 24), (23, 25), (13, 20), (18, 20), (25, 30)],
    'Optimal Humidity': [(93,100), (60, 80), (70, 80), (90,100), (80, 90), (90,100), (90, 100), (50, 60), (80, 90), (80, 90), '-', (95, 100)]
})

print(disease_conditions)


def get_threats(ds,lat,lon,time_start,disease):
    data = ds.sel(latitude=lat, longitude=lon,method = 'nearest')
    date_format = "%Y-%m-%d"
    
    dewpoint = data.sel(time=slice(time_start, datetime.strptime(time_start,date_format) + timedelta(days=10)))['d2m'].values.flatten()-273.15
    temp = data.sel(time=slice(time_start, datetime.strptime(time_start, date_format) + timedelta(days=10)))['t2m'].values.flatten()-273.15
    time = data.sel(time=slice(time_start, datetime.strptime(time_start, date_format) + timedelta(days=10)))['time'].values
    spec_humidity = calculate_relative_humidity(dewpoint+273.15,temp+273.15)
    threat_level = []
# Check for onset threat levels
    for current_temperature,current_humidity in zip(temp,spec_humidity):
        threats = check_progression_threat(current_temperature, current_humidity)
        indicator = threats[threats['Codename']==disease]['Progression Threat Level'].values
        threat_level.append(indicator)

    #print(threat_level)
    print(threat_level)

    threat_level = pd.DataFrame({'Threat':threat_level}).rolling(3).mean()
    threat_level['Time'] = time
    threat_level['Time'] = threat_level['Time'].dt.strftime('%Y-%m-%d %H:%M:%S')
    # Convert dataframe "df" to dictionary
    df_dict = threat_level.dropna(inplace = False).to_dict()
    df_dict['disease'] = disease
    df_dict['lat'] = lat
    df_dict['lon'] = lon

#Store the dictionary into a json string variable in memory
    #df_json = json.dumps(df_dict)

    
    return df_dict


def check_range(value, range_value):
    if isinstance(range_value, tuple):
        return range_value[0] <= value <= range_value[1]
    elif range_value == '-':
        return True
    else:
        return value >= range_value - 2 and value <= range_value + 2

def check_onset_threat(temperature, humidity):
    onset_threat_levels = {}
    
    for _, disease in disease_conditions.iterrows():
        temp_onset = check_range(temperature, disease['Temperature Onset'])
        humi_onset = check_range(humidity, disease['Humidity Onset'])
        
        threat_level = temp_onset and humi_onset
        progression_threat_levels[disease['Codename']] = 1.0 if threat_level else 0.0
        
    return pd.DataFrame(list(onset_threat_levels.items()), columns=['Codename', 'Onset Threat Level'])

def check_progression_threat(temperature, humidity):
    progression_threat_levels = {}
    
    for _, disease in disease_conditions.iterrows():
        temp_optimal = check_range(temperature, disease['Optimal Temperature'])
        humi_optimal = check_range(humidity, disease['Optimal Humidity'])
        
        threat_level = temp_optimal and humi_optimal
        progression_threat_levels[disease['Codename']] = 1.0 if threat_level else 0.0
        
    return pd.DataFrame(list(progression_threat_levels.items()), columns=['Codename', 'Progression Threat Level'])

# Example usage:

    
# Check for progression threat levels
#progression_threats = check_progression_threat(current_temperature, current_humidity)
#print(progression_threats)



class InputData(BaseModel):
    lat: float
    lon: float
    disease: str
    start_datetime: str


@app.get('/')
def read_main():
    return {'message': 'Hello World!'}

@app.post("/process_data")
async def process_data(input_data: InputData):
    # Process the input data and generate the output
    # Replace the following lines with your actual processing logic

    df_json = get_threats(ds,input_data.lat,input_data.lon,str(input_data.start_datetime),input_data.disease)

    return df_json
