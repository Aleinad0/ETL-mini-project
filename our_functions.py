import requests
import os
import json
import datetime
import time
import pandas as pd
import psycopg2
import database_config as dc

def read_api(station):
    current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    selected_station = station   #Vi valde 98210, Andra stationer att leka med 96350,97400,97200
    parameters = range(0,26)


    for param in parameters:
        current_weather_data = fetch_weather_data(selected_station,param)
        try:           
            if current_weather_data:
                filename = f"current_weather_{selected_station}_param{param}_{current_time}.json"
                save_to_file(current_weather_data, filename)
                print(f"Saved current weather data to {filename} in the 'saved_json_files' folder.")
        except Exception as e:
            print(f"Error saving current weather data: {e}")
        time.sleep(0.3)

def harmonize(**kwargs):
    ti = kwargs['ti']
    received_value= ti.xcom_pull(task_ids='list_files')
    list_of_dicts = []

    for path_name in received_value:
        file = f"saved_json_files/{path_name}"
        our_dict = get_data_from_json(file)
        list_of_dicts.append(our_dict)  
    df = pd.DataFrame(list_of_dicts)
    df["Sight"] = df.iloc[0][1]
    df["Current weather"] = df.iloc[1][1]
    df["Air temperature (celcius)"] = df.iloc[2][1]
    df["Wind direction (degrees)"] = df.iloc[3][1]
    df["Wind speed"] = df.iloc[4][1]
    df.drop(["value_name","actual_value"], axis=1, inplace=True)
    df.drop([1,2,3,4,5,6], inplace=True)        
    return df

def clean(**kwargs):
       ti=kwargs['ti']
       resulting_df = ti.xcom_pull(task_ids='harmonize')
       resulting_df["sight_meter"] = resulting_df["Sight"].astype(float)
       resulting_df["current_weather"] = resulting_df["Current weather"]
       resulting_df["air_temperature_celcius"] = resulting_df["Air temperature (celcius)"].astype(float)
       resulting_df["wind_direction_degrees"] = resulting_df["Wind direction (degrees)"].astype(float)
       resulting_df["wind_speed"] = resulting_df["Wind speed"].astype(float)
       resulting_df["station_name"] = resulting_df["station name"]
       resulting_df.drop(columns=["time_of_value_read","Sight","Current weather","Air temperature (celcius)","Wind direction (degrees)","Wind speed","station name"], inplace=True)
       resulting_df.to_json("json_result.json",orient='records', index=False)

def stage():
        conn= psycopg2.connect(
        host = dc.host,
        port = dc.port,
        database = dc.database,
        user= dc.user,
        password = dc.password)

        cur = conn.cursor()
        cur.execute("set search_path to public")
        with open('json_result.json') as file:
            data = file.read()
            query_sql = """
        INSERT INTO weather_info (station_name, latitude, longitude, airflow_execution_date,
        airflow_execution_time,current_weather, sight_meter, air_temperature_celcius, wind_direction_degrees,wind_speed)
        SELECT station_name::VARCHAR(100),
            latitude::REAL,
            longitude::REAL,
            airflow_execution_date::VARCHAR(50),
            airflow_execution_time::VARCHAR(50),
            current_weather::VARCHAR(20),
            sight_meter::REAL,
            air_temperature_celcius::REAL,
            wind_direction_degrees::REAL,
            wind_speed::REAL
        FROM json_populate_recordset(NULL::weather_info, %s);
        """
            cur.execute(query_sql, (data,))
            conn.commit()
            conn.close()


    


def fetch_weather_data(station,parameter):
    endpoint = f"https://opendata-download-metobs.smhi.se/api/version/latest/parameter/{parameter}/station/{station}/period/latest-hour/data.json"
    response = requests.get(endpoint)
    if response.status_code == 200:
        return response.json()
    else:
        print("Error:", response.status_code, response.text)
        return None

def save_to_file(data, filename): 
    folder_name = "saved_json_files"
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    file_path = os.path.join(folder_name, filename)
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def get_timestamp_from_filename(filename):
    split_file_name = filename.split("_")
    time = split_file_name[7].split(".")
    time = time[0]
    date = split_file_name[6]
    return date, time

def get_data_from_json(filename):
    with open(filename, 'r',encoding='utf-8') as file:
        data = json.load(file)
    data
    parameter = data["parameter"]
    value_name = parameter["name"]
    value = data["value"]
    value_list = value[0]
    actual_value = value_list["value"]
    station = data["station"]
    station_name = station["name"]
    from_timestamp = data['period']['from']
    from_datetime = datetime.datetime.utcfromtimestamp(from_timestamp / 1000)
    latitude = data['position'][0]['latitude']
    longitude = data['position'][0]['longitude']
    date, time = get_timestamp_from_filename(filename)      
    
    return {"value_name":value_name,
            "actual_value":actual_value,
            "station name" :station_name,
            "time_of_value_read" : from_datetime,
            "latitude" : latitude,
            "longitude" : longitude,
            "airflow_execution_date" : date,
            "airflow_execution_time" : time 
            }

def list_files(): #take away
    """Return a list of filenames in the given directory."""
    directory_path = "/home/aleinad0/airflow/saved_json_files"
    with os.scandir(directory_path) as entries:
        return [entry.name for entry in entries if entry.is_file()]

