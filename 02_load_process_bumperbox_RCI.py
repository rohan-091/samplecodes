# -*- coding: utf-8 -*-
"""
Created on Wed Nov  3 09:58:00 2021

@author: CM_Rail_2
"""


def load_process(arg1):
    import warnings; warnings.filterwarnings('ignore', 'GeoSeries.isna', UserWarning)
    import pandas as pd
    import datetime as dt
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    from plotly.offline import plot as showplot
    import plotly.express as px
    import numpy as np
    import os
    import glob
    path = os.getcwd()

    #import swifter
    # % Load  data
    os.chdir(path)
    coach_name=arg1
    file_name = 'accelerationdata.parquet'
    data = pd.read_parquet(file_name)


    data['Date_Date'] = pd.to_datetime(data['Time']).dt.date
    data['Date_Time'] = pd.to_datetime(data['Time']).dt.time
    data_operational_days = data['Date_Date'].unique()
    # % create a Dict for each day
    data_day = {}
    for day in data_operational_days:
        df = data.loc[data['Date_Date'] == day]
        key_name=str(day)
        data_day[key_name] = df

    temp_keys=list(data_day.keys())
    del data
#----------------------------- location plot---------------------------
# temp_keys=list(data_day.keys())
# df=data_day[temp_keys[0]]
# df['Speed']=df['Speed']*1.609344*1.609344
# temp_df=df.sample(n=100000)
# temp_figure = px.scatter_mapbox(temp_df, lat="Latitude", lon="Longitude", 
#                                   hover_name="Time",hover_data=['Vehicle','Speed','Vertical','Travel','Lateral'],
#                                   color='Speed',opacity=1, color_continuous_scale =px.colors.sequential.Bluered, zoom=7)
# temp_figure.update_layout(mapbox_style="open-street-map")
# temp_figure.update_layout(margin={"r":0,"t":0,"l":0,"b":0},font=dict(family="Arial",size=24,color="RebeccaPurple"))
# temp_Name='Accelerationbox_data_0.html'
# showplot(temp_figure, filename = temp_Name, auto_open=True)

# -----------------------------Comfort Calculation -----------------------
# Caclulate Ride comfort for each day using comfpy, might need to do some modificiations
# change directory to import comfpy files and then copy the code for compfy calculation, deal with missing importation
    path = os.getcwd()
    comfpy_path = "C:/Users/CM_Rail_2/OneDrive - KTH/KTH+SJ+PPM/06_RK_Work/06_pythonlibraries/comfpy-master"
    os.chdir(comfpy_path)
    import comfpy
    from comfpy import en12299
    import comfpy.wz
    import matplotlib.pyplot as plt
    import time
    import numpy as np
    os.chdir(path)
#------------------Comfort calculation-----------------------------------------
    channels = {}
    temp_keys = list(data_day.keys())
    data_day_Comfort = {}
    for i in range(0, np.size(temp_keys)):
        day = temp_keys[i]
        channels = {'x': {'1': data_day[day].Vertical*9.8},
                    'y': {'1': data_day[day].Travel*9.8},
                    'z': {'1': data_day[day].Lateral*9.8-9.8},
                    'VehicleTag': {'1': np.array(data_day[day].Vehicle)},
                    'time': {'1': np.array(data_day[day].Time)},
                    'dates': {'1': np.array(data_day[day].Date_Date)},
                    'Speed': {'1': data_day[day].Speed},
                    'Latitude': {'1': data_day[day].Latitude},
                    'Longitude': {'1': data_day[day].Longitude}}
        if len(channels['x']['1']) >2080:
            f = comfpy.en12299.en12299(fs=208, channels=channels, analyse='full')
            comfort_en12299, comfort_en12299_NmV = f.get('1', 'cc')
            f = comfpy.wz.wz(fs=208, channels=channels, analyse='full')
            comfort_wz = f.get('1', 'wz')
        else:
            comfort_en12299 = pd.DataFrame([])
            comfort_en12299_NmV = pd.DataFrame([])
            comfort_wz = pd.DataFrame([])
        
        data_day_Comfort[day] = {'comfort_en12299': comfort_en12299,
                                  'comfort_en12299_NmV': comfort_en12299_NmV,
                                  'comfort_wz': comfort_wz}
    # calculate df for each data Wz and filter and NmV and store as dict.
# # %% visualization of Wz y of 
# fig,ax=plt.subplots()
# for keys in data_day_Comfort.keys():
#     df2=data_day_Comfort[keys]
#     df3=df2['comfort_wz']
#     df3.hist(column="y", ax=ax);
# fig,ax=plt.subplots()
# for keys in data_day_Comfort.keys():
#     df2=data_day_Comfort[keys]
#     df3=df2['comfort_wz']
#     df3.hist(column="x", ax=ax);
    del data_day
# storing results in SQL database
#--------------create a database -------------------#
    # import the mysql client for python
    import pymysql
    # Create a connection object
    databaseServerIP            = "localhost"  # IP address of the MySQL database server
    databaseUserName            = "root"       # User name of the database server
    databaseUserPassword        = "password"           # Password for the database user
    newDatabaseName             = "databasename" # Name of the database that is to be created
    charSet                     = "utf8mb4"     # Character set
    cusrorType                  = pymysql.cursors.DictCursor
    connectionInstance   = pymysql.connect(host=databaseServerIP,
                                            user=databaseUserName,
                                            password=databaseUserPassword,
                                            charset=charSet,cursorclass=cusrorType)
    try:
        # Create a cursor object
        cursorInsatnce = connectionInstance.cursor()                        
        # SQL Statement to create a database
        sqlStatement = "CREATE DATABASE " + newDatabaseName
        # Execute the create database SQL statment through the cursor instance
        cursorInsatnce.execute(sqlStatement)
        # SQL query string
        sqlQuery            = "SHOW DATABASES"
        # Execute the sqlQuery
        cursorInsatnce.execute(sqlQuery)
        #Fetch all the rows
        databaseList                = cursorInsatnce.fetchall()
        for datatbase in databaseList:
            print(datatbase)
    except Exception as e:
        print("Exeception occured:{}".format(e))
    finally:
        connectionInstance.close()
    
    #--------------create a database engine -------------------#
    
    from sqlalchemy import create_engine
    # Create SQLAlchemy engine to connect to MySQL Database
    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                            .format(host="localhost", db=newDatabaseName, 
                                    user=databaseUserName, pw=databaseUserPassword))
#------------------sending data to sql database----------------------------
# how to handel emppty dataframes here??
    for keys in data_day_Comfort.keys():
        comfort_wz=data_day_Comfort[keys]['comfort_wz']
        comfort_en12299=data_day_Comfort[keys]['comfort_en12299']
        comfort_en12299_NmV=data_day_Comfort[keys]['comfort_en12299_NmV']    
        wz_name= coach_name +'_' + str(keys) + '_comfort_wz'
        comfort_en12299_name= coach_name +'_' +str(keys) + '_comfort_en12299'
        comfort_en12299_NmV_name= coach_name +'_' + str(keys) + '_comfort_en12299_NmV'
        if len(comfort_wz.index) != 0:
            comfort_wz.to_sql(wz_name, engine, index=False,if_exists='replace')
        if len(comfort_en12299.index) != 0:
            comfort_en12299.to_sql(comfort_en12299_name, engine, index=False,if_exists='replace')
        if len(comfort_en12299_NmV.index) != 0:
            comfort_en12299_NmV.to_sql(comfort_en12299_NmV_name, engine, index=False,if_exists='replace')

# --------------------------- save dict as json file ----------------
    import json
    class JSONEncoder(json.JSONEncoder):
        def default(self, obj):
            if hasattr(obj, 'to_json'):
                return obj.to_json(orient='records')
            return json.JSONEncoder.default(self, obj)
    json_name = coach_name+'_.json'
    with open(json_name, 'w') as fp:
        json.dump(data_day_Comfort, fp, cls=JSONEncoder, indent=4)
    

# %% Then I use this file as script for all other files

AA = load_process('CoachID1')
AA = load_process('CoachID2')
AA = load_process('CoachID3')
AA = load_process('CoachID4') 
AA = load_process('CoachID5')

# %% Open read data from SQL table and plot via plotly
#---------------------list names of tables ---------------------
import pymysql
import numpy as np

connection = pymysql.connect(
    host = 'localhost',
    user = 'root',
    passwd = 'password')  # create the connection

cursor = connection.cursor()     # get the cursor
cursor.execute("USE DATABASE") # select the database
cursor.execute("SHOW TABLES")    # execute 'SHOW TABLES' (but data is not returned)
tables = cursor.fetchall()       # return data from last query
idx_CoachID1_wz=0;idx_CoachID2_wz=0;idx_CoachID3_wz=0;idx_CoachID4_wz=0;idx_CoachID5_wz=0;
idx_CoachID1_NmV=0;idx_CoachID2_NmV=0;idx_CoachID3_NmV=0;idx_CoachID4_NmV=0;idx_CoachID5_NmV=0;
idx_CoachID1_EN12299=0;idx_CoachID2_EN12299=0;idx_CoachID3_EN12299=0;idx_CoachID4_EN12299=0;idx_CoachID5_EN12299=0;
CoachID1_wz={}; CoachID2_wz={}; CoachID3_wz={}; CoachID4_wz={}; CoachID5_wz={}; 
CoachID1_NmV={}; CoachID2_NmV={}; CoachID3_NmV={}; CoachID4_NmV={}; CoachID5_NmV={}; 
CoachID1_EN12299={}; CoachID2_EN12299={}; CoachID3_EN12299={}; CoachID4_EN12299={}; CoachID5_EN12299={}; 

for idx in range(0,np.size(tables)):
    s=tables[idx][0]
    if "CoachID1" in s:
        if "wz" in s:
            CoachID1_wz[idx_CoachID1_wz]=s
            idx_CoachID1_wz=idx_CoachID1_wz+1
        elif "_comfort_en12299_nmv" in s:
            CoachID1_NmV[idx_CoachID1_NmV]=s
            idx_CoachID1_NmV=idx_CoachID1_NmV+1  
    elif "CoachID2" in s:
        if "wz" in s:
            CoachID2_wz[idx_CoachID2_wz]=s
            idx_CoachID2_wz=idx_CoachID2_wz+1
        elif "_comfort_en12299_nmv" in s:
            CoachID2_NmV[idx_CoachID2_NmV]=s
            idx_CoachID2_NmV=idx_CoachID2_NmV+1
    elif "CoachID3" in s:
        if "wz" in s:
            CoachID3_wz[idx_CoachID3_wz]=s
            idx_CoachID3_wz=idx_CoachID3_wz+1
        elif "_comfort_en12299_nmv" in s:
            CoachID3_NmV[idx_CoachID3_NmV]=s
            idx_CoachID3_NmV=idx_CoachID3_NmV+1
    elif "CoachID4" in s:
        if "wz" in s:
            CoachID4_wz[idx_CoachID4_wz]=s
            idx_CoachID4_wz=idx_CoachID4_wz+1
        elif "_comfort_en12299_nmv" in s:
            CoachID4_NmV[idx_CoachID4_NmV]=s
            idx_CoachID4_NmV=idx_CoachID4_NmV+1
    elif "CoachID5" in s:
        if "wz" in s:
            CoachID5_wz[idx_CoachID5_wz]=s
            idx_CoachID5_wz=idx_CoachID5_wz+1
        elif "_comfort_en12299_nmv" in s:
            CoachID5_NmV[idx_CoachID5_NmV]=s
            idx_CoachID5_NmV=idx_CoachID5_NmV+1


#------------------retrive data in dataframe -------------------
from sqlalchemy import create_engine
import pandas as pd
db_connection = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                       .format(host="localhost", db="x2_sept_21_rci", 
                               user="root", pw="password"))

#--------------call data for al wz ------------------------
df_CoachID1_wz=pd.DataFrame([])
for table_name in CoachID1_wz.values():
    SQL_query="SELECT * FROM " + "`"+str(table_name) + "`"
    df = pd.read_sql(SQL_query, con=db_connection)
    df_CoachID1_wz=df_CoachID1_wz.append(df)

df_CoachID2_wz=pd.DataFrame([])
for table_name in CoachID2_wz.values():
    SQL_query="SELECT * FROM " + "`"+str(table_name) + "`"
    df = pd.read_sql(SQL_query, con=db_connection)
    df_CoachID2_wz=df_CoachID2_wz.append(df)

df_CoachID3_wz=pd.DataFrame([])
for table_name in CoachID3_wz.values():
    SQL_query="SELECT * FROM " + "`"+str(table_name) + "`"
    df = pd.read_sql(SQL_query, con=db_connection)
    df_CoachID3_wz=df_CoachID3_wz.append(df)

df_CoachID4_wz=pd.DataFrame([])
for table_name in CoachID4_wz.values():
    SQL_query="SELECT * FROM " + "`"+str(table_name) + "`"
    df = pd.read_sql(SQL_query, con=db_connection)
    df_CoachID4_wz=df_CoachID4_wz.append(df)

df_CoachID5_wz=pd.DataFrame([])
for table_name in CoachID5_wz.values():
    SQL_query="SELECT * FROM " + "`"+str(table_name) + "`"
    df = pd.read_sql(SQL_query, con=db_connection)
    df_CoachID5_wz=df_CoachID5_wz.append(df)

#%% sending data to csv

df_CoachID1_wz.to_csv('df_CoachID1_wz.csv', encoding="utf-8")
df_CoachID2_wz.to_csv('df_CoachID2_wz.csv', encoding="utf-8")
df_CoachID3_wz.to_csv('df_CoachID3_wz.csv', encoding="utf-8")
df_CoachID4_wz.to_csv('df_CoachID4_wz.csv', encoding="utf-8")
df_CoachID5_wz.to_csv('df_CoachID5_wz.csv', encoding="utf-8")
#%%------------------------plotting-------------------------------
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from plotly.offline import plot as showplot
import plotly.express as px

# temp_figure = px.scatter_mapbox(df, lat="Latitude", lon="Longitude", 
#                                   hover_name="time",hover_data=['VehicleTag','Speed','x','y','z'],
#                                   color='y',opacity=1, color_continuous_scale =px.colors.sequential.Bluered, zoom=7)
# temp_figure.update_layout(mapbox_style="open-street-map")
# temp_figure.update_layout(margin={"r":0,"t":0,"l":0,"b":0},font=dict(family="Arial",size=24,color="RebeccaPurple"))
# temp_figure = px.line(df, x="time", y=["x","y","z"])
temp_figure = px.line(df, x="time", y=["Latitude","Longitude"])

temp_Name='Accelerationbox_data_0.html'
showplot(temp_figure, filename = temp_Name, auto_open=True)

# %% Correct the GPS loss with WSN data

import pandas as pd
import os
path = os.getcwd()
os.chdir(path)
file_name = 'SJ_WSN_Export_September_2021.parquet'
SJ_WSN_September_2021 = pd.read_parquet(file_name)

SJ_WSN_September_2021['Date_Date'] = pd.to_datetime(SJ_WSN_September_2021['Time']).dt.date
SJ_WSN_September_2021['Date_Time'] = pd.to_datetime(SJ_WSN_September_2021['Time']).dt.time
data_operational_days = SJ_WSN_September_2021['Date_Date'].unique()
data_operational_vehicles=SJ_WSN_September_2021['Vehicle'].unique()
# % create a Dict for each vehicle and each day
SJ_WSN_September_2021_sorted = {}

for vehicle in data_operational_vehicles:
    df=SJ_WSN_September_2021.loc[SJ_WSN_September_2021['Vehicle'] == vehicle]
    key_vehicle=str(vehicle)
    SJ_WSN_September_2021_sorted[key_vehicle]={}
    for day in data_operational_days:
        df2 = df.loc[df['Date_Date'] == day]
        key_day=str(day)
        SJ_WSN_September_2021_sorted[key_vehicle][key_day] = df2

del df, df2, day, vehicle, key_day, key_vehicle

#------------------------saving in json-------------------------------
import json
class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'to_json'):
            return obj.to_json(orient='records')
        return json.JSONEncoder.default(self, obj)
json_name = 'SJ_WSN_September_2021_sorted.json'
with open(json_name, 'w') as fp:
    json.dump(SJ_WSN_September_2021_sorted, fp, cls=JSONEncoder, indent=4)

#------------------------plotting WSN data-------------------------------
df= SJ_WSN_September_2021_sorted['CoachID1']['2021-09-14']
temp_figure = px.scatter_mapbox(df, lat="Lat", lon="Long", 
                                  hover_name="Time",hover_data=['Vehicle'],
                                  color='RMSY',opacity=1, color_continuous_scale =px.colors.sequential.Bluered, zoom=7)
temp_figure.update_layout(mapbox_style="open-street-map")
temp_figure.update_layout(margin={"r":0,"t":0,"l":0,"b":0},font=dict(family="Arial",size=24,color="RebeccaPurple"))
# temp_figure = px.line(df, x="Time", y=["Lat","Long"])
temp_Name='Accelerationbox_data_0_wsn.html'
showplot(temp_figure, filename = temp_Name, auto_open=True)
df_wsn=df
del df


