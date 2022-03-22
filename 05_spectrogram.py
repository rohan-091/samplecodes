# -*- coding: utf-8 -*-
"""
Created on Tue Feb  1 16:23:04 2022

@author: CM_Rail
"""
#%%
#This script is developed to calculate the spectrograms of the carbody floor acceleration of each journey of train. Then store these spectrograms in sql database and also in json file.
#%%
# import the libraries

def spectrogram_X2(IP1, IP2, IP3):
    import scipy.signal as sc_signal
    import matplotlib.mlab as win_lab
    import numpy as np
    import pandas as pd
    # function input
    #IP1: Signal
    #IP2=sampling frequency
    #IP3= time window
    P4=IP3*IP2
    P5=sc_signal.get_window('hamming', P4)
    P6=P4/2
    res = {}
    for key in IP1.keys():
        res[key] = {'f': {},
                    't': {},
                    'Sxx': {}}
        f, t, Spec_xx = sc_signal.spectrogram(IP1[key]['1'], nfft=P4, fs=IP2,
                                          window=P5, noverlap=P6)
        res[key]['f']=f
        res[key]['t']=t
        res[key]['Sxx']=Spec_xx              

    res_export = {'f': {'x':{},
                        'y':{},
                        'z':{}},
                  't': {'x':{},
                      'y':{},
                      'z':{}},
                  'Sxx': {'x':{},
                        'y':{},
                        'z':{}}}
    for key in res.keys():
        res_export['f'][key] = res[key]['f']
        res_export['t'][key] = res[key]['t']
        res_export['Sxx'][key] = res[key]['Sxx']
    Freq=pd.Series(res_export['f']['x'])
    Time=pd.Series(res_export['t']['x'])
    Sxx=pd.DataFrame(res_export['Sxx']['x'])
    Syy=pd.DataFrame(res_export['Sxx']['y'])
    Szz=pd.DataFrame(res_export['Sxx']['z'])
    Sxx.set_index([Freq])
    Sxx.columns =[Time]
    Syy.set_index([Freq])
    Syy.columns =[Time]
    Szz.set_index([Freq])
    Szz.columns =[Time]
    
    return Sxx, Syy, Szz 
    
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
    file_name = 'AccelerationFile.parquet'
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
#----------------------------- location plot using scatter_mapbox---------------------------
# temp_keys=list(data_day.keys())
# df=data_day[temp_keys[0]]
# df['Speed']=df['Speed']*1.609344*1.609344
# temp_df=df.sample(n=100000)
# temp_figure = px.scatter_mapbox(temp_df, lat="Latitude", lon="Longitude", 
#                                   hover_name="Time",hover_data=['Vehicle','Speed','Vertical','Travel','Lateral'],
#                                   color='Speed',opacity=1, color_continuous_scale =px.colors.sequential.Bluered, zoom=7)
# temp_figure.update_layout(mapbox_style="open-street-map")
# temp_figure.update_layout(margin={"r":0,"t":0,"l":0,"b":0},font=dict(family="Arial",size=24,color="RebeccaPurple"))
# temp_Name='Bumperbox_data_0.html'
# showplot(temp_figure, filename = temp_Name, auto_open=True)

# -----------------------------spectrum calculation -----------------------
# Caclulate spectrogram for each day using spectrogram, might need to do some modificiations
#------------------Spectra calculation-----------------------------------------
    channels = {}
    temp_keys = list(data_day.keys())
    data_day_spectra = {}
    for i in range(0, np.size(temp_keys)):
        day = temp_keys[i]
        channels = {'x': {'1': data_day[day].Vertical*9.8},
                    'y': {'1': data_day[day].Travel*9.8},
                    'z': {'1': data_day[day].Lateral*9.8-9.8}}
        

        if len(channels['x']['1']) >2080:
            spectra_x, spectra_y, spectra_z = spectrogram_X2(channels, 208, 10)
        else:
            spectra_x = pd.DataFrame([])
            spectra_y = pd.DataFrame([])
            spectra_z = pd.DataFrame([])
        
        data_day_spectra[day] = { 'spectra_x': spectra_x,
                                  'spectra_y': spectra_y,
                                  'spectra_z': spectra_z}
    del data_day
# storing results in SQL database
#--------------create a database -------------------#
    # import the mysql client for python
    import pymysql
    # Create a connection object
    databaseServerIP            = "localhost"  # IP address of the MySQL database server
    databaseUserName            = "root"       # User name of the database server
    databaseUserPassword        = "password"           # Password for the database user
    newDatabaseName             = "database_name" # Name of the database that is to be created
    charSet                     = "utf8mb4"     # Character set
    cusrorType                  = pymysql.cursors.DictCursor
    connectionInstance   = pymysql.connect(host=databaseServerIP,
                                            user=databaseUserName,
                                            password=databaseUserPassword,
                                            charset=charSet,cursorclass=cusrorType)
    try:
        # Create a cursor object
        cursorInsatnce = connectionInstance.cursor()
        # SQL statement for dropping
        sqlStatement = "DROP DATABASE " + newDatabaseName                        
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
    for keys in data_day_spectra.keys():
        spectra_x=data_day_spectra[keys]['spectra_x']
        spectra_y=data_day_spectra[keys]['spectra_y'].astype(np.float16)
        spectra_z=data_day_spectra[keys]['spectra_z']    
        spectra_x_name= coach_name +'_' + str(keys) + '_spectra_x'
        spectra_y_name= coach_name +'_' + str(keys) + '_spectra_y'
        spectra_z_name= coach_name +'_' + str(keys) + '_spectra_z'
        if len(spectra_x.index) != 0:
            spectra_x.to_sql(spectra_x_name.lower(), engine, index=True,if_exists='replace')
        if len(spectra_y.index) != 0:
            spectra_y.to_sql(spectra_y_name.lower(), engine, index=False,if_exists='replace')
        if len(spectra_z.index) != 0:
            spectra_z.to_sql(spectra_z_name.lower(), engine, index=False,if_exists='replace')

# # --------------------------- save dict as json file ----------------
    import json
    class JSONEncoder(json.JSONEncoder):
        def default(self, obj):
            if hasattr(obj, 'to_json'):
                return obj.to_json(orient='records')
            return json.JSONEncoder.default(self, obj)
    json_name = coach_name+'_spectra.json'
    with open(json_name, 'w') as fp:
        json.dump(data_day_spectra, fp, cls=JSONEncoder, indent=4)
    

# %% Then I use this file as script for all other files

AA = load_process('CoachID1')
AA = load_process('CoachID2')
AA = load_process('CoachID3')
AA = load_process('CoachID4') 
AA = load_process('CoachID5')