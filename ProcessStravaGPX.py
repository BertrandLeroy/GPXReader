
# finding the right modules/packages to use is not easy 
import os 
import pyodbc 
from numpy import genfromtxt
import pandas as pd
import sqlalchemy as sa # use sqlalchemy for truncating etc
from sqlalchemy import Column, Integer, Float, Date, String, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects import mssql

import urllib
import csv
import fnmatch2 
from time import time


## DATABASE STUFF
# server + instance = DESKTOP-A6J6D7Q\LEROYB_INSTANCE
# database = Tracks

server_name='DESKTOP-A6J6D7Q\LEROYB_INSTANCE'
db_name='Tracks'

path = 'F:\export_6642035' # location of all the files related to STRAVA export
pathArchive = 'F:\export_6642035_archive' # this is the archive folder that will contain all the processed files
searchstring = '*' # we could limit the files to a particular string pattern, however in this case we want to go through all the files

# https://docs.sqlalchemy.org/en/13/dialects/mssql.html#connecting-to-pyodbc
# https://docs.sqlalchemy.org/en/13/core/connections.html
# https://www.pythonsheets.com/notes/python-sqlalchemy.html
# https://auth0.com/blog/sqlalchemy-orm-tutorial-for-python-developers/
# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.truncate.html

params = urllib.parse.quote_plus('Driver={SQL Server};'
'Server='+server_name +';'
'Database='+db_name+';'
'Trusted_Connection=yes;')

engine = sa.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params,fast_executemany=True, echo=True)

# https://chrisalbon.com/python/data_wrangling/pandas_dataframe_importing_csv/
def Load_Data(file_name):
    #data = genfromtxt(file_name, delimiter=',', skip_header=1, converters={0: lambda s: str(s)})
    data = pd.read_csv(file_name, skiprows=0)
    return data.values.tolist()

Base = declarative_base()
BaseGPX = declarative_base()

class Activities(Base):
    #Tell SQLAlchemy what the table name is and if there's any table-specific arguments it should know about
    __tablename__ = 'Activities'
    __table_args__ = {'schema':'STG'}
    #tell SQLAlchemy the name of column and its attributes:
    Activity_ID = Column(BigInteger, primary_key=True, nullable=False) #
    Activity_Date = Column(Date)
    Activity_Name = Column(String)
    Activity_Type = Column(String)
    Activity_Description = Column(String)
    Elapsed_Time = Column(Integer)
    Distance = Column(Float)
    Commute = Column(String)
    Activity_Gear = Column(String)
    Filename = Column(String)

class GPXFiles(BaseGPX):
    #Tell SQLAlchemy what the table name is and if there's any table-specific arguments it should know about
    __tablename__ = 'GPXFiles'
    __table_args__ = {'schema':'STG'}
    #tell SQLAlchemy the name of column and its attributes:
    GPXFile_ID = Column(BigInteger, primary_key=True, autoincrement=True, nullable=False) #
    GPXFile_Name = Column(String, nullable=True)
    GPXFile_Contents = Column(String, nullable=True)
    #GPXFile_XMLContents = Column(String, nullable=True)
    #GPXFile_Route_ID = Column(BigInteger, nullable=True)


# loop through files
# r=root, d=directories, f = files
for r, d, f in os.walk(path):
    for file in f:
        if fnmatch2.fnmatch2(file, searchstring):

            if file.endswith('.gpx'):
                print('gpxfile')
                # go straight into the STG.GPXFiles table
                #Create the session
                #GPXFiles.__table__.drop(bind=engine)
                GPXFiles.__table__.create(bind=engine, checkfirst=True) # if the table exists, it is dropped (not using checkfirts=true)
                session = sessionmaker(bind=engine)
                #print(session)
                session.configure(bind=engine)
                s = session()
                try:

                    data = Load_Data(os.path.join(r, file)) 
                    #print(data)
                    #for i in data:
                        #print(str(i[0])+'-'+str(i[1]))
                    record = GPXFiles(**{
                        #'GPXFile_ID' : i[0],
                        'GPXFile_Name' : file, #str(i[1]),
                        'GPXFile_Contents' : data
                    })

                    s.add(record) #Add all the records

                    s.commit() #Attempt to commit all the records

                except Exception as e: 
                    print(e)
                    s.rollback() #Rollback the changes on error
                    
                s.close() #Close the connection
                continue
            elif file.endswith('.gpx.gz'):
                # print('a GZ gpxfile')
                # unzip file first, then get the file into the STG.GPXFiles table
                continue
            elif file.endswith('.csv'):
                # the only file we really need is the activities.csv file
                # the columns are 
                if file == 'activities.csv':
                    # import it

                    ### using pure panda
                    # insert CSV into the table
                    # https://stackoverflow.com/questions/31394998/using-sqlalchemy-to-load-csv-file-into-a-database

                    #Create the session
                    #Activities.__table__.drop(bind=engine)
                    Activities.__table__.create(bind=engine, checkfirst=True) # if the table exists, it is dropped (not using checkfirts=true)
                    session = sessionmaker(bind=engine)
                    print(session)
                    session.configure(bind=engine)
                    s = session()
                    try:

                        data = Load_Data(os.path.join(r, file)) 
                        #print(data)
                        for i in data:
                            #print(str(i[0])+'-'+str(i[1]))
                            record = Activities(**{
                                'Activity_ID' : i[0],
                                'Activity_Date' : i[1].replace(',', ''),
                                'Activity_Name' : str(i[2]),
                                'Activity_Type' : str(i[3]),
                                'Activity_Description' : str(i[4]),
                                'Elapsed_Time' : int(i[5]),
                                'Distance' : float(i[6].replace(',', '')),
                                'Commute' : str(i[7]),
                                'Activity_Gear' : str(i[8]),
                                'Filename' : str(i[9])
                            })

                            s.add(record) #Add all the records

                            s.commit() #Attempt to commit all the records

                    except Exception as e: 
                        print(e)
                        s.rollback() #Rollback the changes on error
                    
                    s.close() #Close the connection
                    
            else:
                continue

            # move file to archive folder at same level as source folder 
            # first check folder exist, if not, then create it

        continue

#conn.close

# now that this is done, we need to run the stored procedures to get the data from the staging tbales into the production conformed tables
# first to brng the data from staging into conformed area
# then run a bunch of procs to update some of th emissing attributes

# don't forget to clean up in some way



