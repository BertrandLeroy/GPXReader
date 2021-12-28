import os
import pandas as pd
import numpy as np
import pyodbc

def importActivitiesFileIntoDB(file_name):

    print(pyodbc.version)


    server_name='DESKTOP-A6J6D7Q\LEROYB_INSTANCE'
    db_name='Tracks'
    
    data = pd.read_csv (file_name)   
    df = pd.DataFrame(data, columns= ['Activity ID','Activity Date','Activity Name','Activity Type','Elapsed Time','Distance','Relative Effort','Commute','Activity Gear','Filename','Athlete Weight','Bike Weight','Moving Time','Max Speed','Average Speed','Elevation Gain','Elevation Loss','Elevation Low','Elevation High','Max Grade','Average Grade','Average Positive Grade','Average Negative Grade','Average Cadence','Max Cadence','Max Heart Rate','Average Heart Rate','Max Watts','Average Watts','Calories','Max Temperature','Average Temperature','Total Work','Number of Runs','Uphill Time','Downhill Time','Other Time','Perceived Exertion','Weighted Average Power','Power Count','Prefer Perceived Exertion','Perceived Relative Effort','Total Weight Lifted','From Upload','Grade Adjusted Distance','Weather Observation Time','Weather Condition','Weather Temperature','Apparent Temperature','Dewpoint','Humidity','Weather Pressure','Wind Speed','Wind Gust','Wind Bearing','Precipitation Intensity','Sunrise Time','Sunset Time','Moon Phase','Bike','Gear','Precipitation Probability','Precipitation Type','Cloud Cover','Weather Visibility','UV Index','Weather Ozone'])
    
## Below are the columns available in the activities file from Strava
#Activity ID
#Activity Date
#Activity Name
#Activity Type
#Activity Description
#Elapsed Time
#Distance
#Relative Effort
#Commute
#Activity Gear
#Filename
#Athlete Weight
#Bike Weight
#Elapsed Time
#Moving Time
#Distance
#Max Speed
#Average Speed
#Elevation Gain
#Elevation Loss
#Elevation Low
#Elevation High
#Max Grade
#Average Grade
#Average Positive Grade
#Average Negative Grade
#Average Cadence
#Max Cadence
#Max Heart Rate
#Average Heart Rate
#Max Watts
#Average Watts
#Calories
#Max Temperature
#Average Temperature
#Relative Effort
#Total Work
#Number of Runs
#Uphill Time
#Downhill Time
#Other Time
#Perceived Exertion
#Weighted Average Power
#Power Count
#Prefer Perceived Exertion
#Perceived Relative Effort
#Commute
#Total Weight Lifted
#From Upload
#Grade Adjusted Distance
#Weather Observation Time
#Weather Condition
#Weather Temperature
#Apparent Temperature
#Dewpoint
#Humidity
#Weather Pressure
#Wind Speed
#Wind Gust
#Wind Bearing
#Precipitation Intensity
#Sunrise Time
#Sunset Time
#Moon Phase
#Bike
#Gear
#Precipitation Probability
#Precipitation Type
#Cloud Cover
#Weather Visibility
#UV Index
#Weather Ozone
    
    
    
    
    #Activity ID,Activity Date,Activity Name,Activity Type,Activity Description,Elapsed Time,Distance,Commute,Activity Gear,Filename
    #to_drop = ['Activity Description']
    #df.drop(columns=to_drop, inplace=True)
    df=df.fillna(0)
    #df=df.str.replace('"','')
    #df=df.replace('\'','')
    #df = df.applymap(lambda x: x.replace('.gz', ''))
    #df['Filename'].str.replace('activities/','')
    #df=df.str.replace('','')
    #df=df.str.replace('activities/','')
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')
    
    #print(df)

    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=DESKTOP-A6J6D7Q\LEROYB_INSTANCE;'
                      'Database=Tracks;'
                      'Trusted_Connection=yes;')
    cursor = conn.cursor()

    for row in df.itertuples():

        try:
            cursor.execute('''
                INSERT INTO Tracks.STG.Activities ([Activity_ID], [Activity_Date],[Activity_Name],[Activity_Type],[Elapsed_Time],[Distance],[Commute],[Activity_Gear],[Filename] 
                ,[Relative_Effort]
                ,[Athlete_Weight]
                ,[Bike_Weight]
                ,[Moving_Time]
                ,[Max_Speed]
                ,[Average_Speed]
                ,[Elevation_Gain]
                ,[Elevation_Loss]
                ,[Elevation_Low]
                ,[Elevation_High]
                ,[Max_Grade]
                ,[Average_Grade]
                ,[Average_Positive_Grade]
                ,[Average_Negative_Grade]
                ,[Average_Cadence]
                ,[Max_Cadence]
                ,[Max_Heart_Rate]
                ,[Average_Heart_Rate]
                ,[Max_Watts]
                ,[Average_Watts]
                ,[Calories]
                ,[Max_Temperature]
                ,[Average_Temperature]
                ,[Total_Work]
                ,[Number_of_Runs]
                ,[Uphill_Time]
                ,[Downhill_Time]
                ,[Other_Time]
                ,[Perceived_Exertion]
                ,[Weighted_Average_Power]
                ,[Power_Count]
                ,[Prefer_Perceived_Exertion]
                ,[Perceived_Relative_Effort]
                ,[Total_Weight_Lifted]
                ,[From_Upload]
                ,[Grade_Adjusted_Distance]
                ,[Weather_Observation_Time]
                ,[Weather_Condition]
                ,[Weather_Temperature]
                ,[Apparent_Temperature]
                ,[Dewpoint]
                ,[Humidity]
                ,[Weather_Pressure]
                ,[Wind_Speed]
                ,[Wind_Gust]
                ,[Wind_Bearing]
                ,[Precipitation_Intensity]
                ,[Sunrise_Time]
                ,[Sunset_Time]
                ,[Moon_Phase]
                ,[Bike]
                ,[Gear]
                ,[Precipitation_Probability]
                ,[Precipitation_Type]
                ,[Cloud_Cover]
                ,[Weather_Visibility]
                ,[UV_Index]
                ,[Weather_Ozone]



                   )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ''',
                #,[Activity_Description]   ,?,?,?,?,?,?,? 

                row.activity_id
                ,row.activity_date#.replace(',', '')
                ,row.activity_name
                ,row.activity_type
                ##,row.activity_description
                ,row.elapsed_time
                ,row.distance#.replace(',', '') 
                ,row.commute
                ,row.activity_gear
                ,row.filename
                ,row.relative_effort
                ,row.athlete_weight
                ,row.bike_weight
                ,row.moving_time
                ,row.max_speed
                ,row.average_speed
                ,row.elevation_gain
                ,row.elevation_loss
                ,row.elevation_low
                ,row.elevation_high
                ,row.max_grade
                ,row.average_grade
                ,row.average_positive_grade
                ,row.average_negative_grade
                ,row.average_cadence
                ,row.max_cadence
                ,row.max_heart_rate
                ,row.average_heart_rate
                ,row.max_watts
                ,row.average_watts
                ,row.calories
                ,row.max_temperature
                ,row.average_temperature
                ,row.total_work
                ,row.number_of_runs
                ,row.uphill_time
                ,row.downhill_time
                ,row.other_time
                ,row.perceived_exertion
                ,row.weighted_average_power
                ,row.power_count
                ,row.prefer_perceived_exertion
                ,row.perceived_relative_effort
                ,row.total_weight_lifted
                ,row.from_upload
                ,row.grade_adjusted_distance
                ,row.weather_observation_time
                ,row.weather_condition
                ,row.weather_temperature
                ,row.apparent_temperature
                ,row.dewpoint
                ,row.humidity
                ,row.weather_pressure
                ,row.wind_speed
                ,row.wind_gust
                ,row.wind_bearing
                ,row.precipitation_intensity
                ,row.sunrise_time
                ,row.sunset_time
                ,row.moon_phase
                ,row.bike
                ,row.gear
                ,row.precipitation_probability
                ,row.precipitation_type
                ,row.cloud_cover
                ,row.weather_visibility
                ,row.uv_index
                ,row.weather_ozone


                )
        except Exception as e: 
            print(row)
            print(e)

    conn.commit()

#def ImportGPXFileIntoDB(file_name):
#    server_name='DESKTOP-A6J6D7Q\LEROYB_INSTANCE'
#    db_name='Tracks'

#    f = open(file_name,"r")
#    string = f.read()
    
#    conn = pyodbc.connect('Driver={SQL Server};'
#                      'Server=DESKTOP-A6J6D7Q\LEROYB_INSTANCE;'
#                      'Database=Tracks;'
#                      'Trusted_Connection=yes;')
#    cursor = conn.cursor()

#    try:
#        cursor.execute('''
#            INSERT INTO Tracks.STG.GPXFiles ([GPXFile_Name],[GPXFile_Contents])
#            VALUES (?,?)
#            ''',
#            file_name, string
#            )
#    except Exception as e: 
#        print(row)
#        print(e)

#    conn.commit()

def ImportGPXFileIntoDB(dir_name):
    server_name='DESKTOP-A6J6D7Q\LEROYB_INSTANCE'
    db_name='Tracks'
    GPXextension = '.gpx'

    os.chdir(dir_name) # change directory from working dir to dir with files

    for item in os.listdir(dir_name): # loop through items in dir
        if item.endswith(GPXextension): # check for ".zip" extension
            file_name = os.path.abspath(item) # get full path of files
            
            #ImportFilesIntoDB.ImportGPXFileIntoDB(file_name)
            f = open(file_name,"r")
            string = f.read()            
            f.close() # close file

            conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                              'Server=DESKTOP-A6J6D7Q\LEROYB_INSTANCE;'
                              'Database=Tracks;'
                              'Trusted_Connection=yes;')
            cursor = conn.cursor()

            #print(string)
            print(file_name)

            try:
                cursor.execute('''
                    INSERT INTO Tracks.STG.GPXFiles ([GPXFile_Name],[GPXFile_Contents])
                    VALUES (?,?)
                    ''',
                    file_name, string
                    )
                
            except Exception as e: 
                print(file_name)
                print(e)

            conn.commit()
    
            #os.remove(file_name) # delete zipped file




    
    
