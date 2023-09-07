## program used to get Strava files into a SQL server database 
## the database and required tables need to exist beforehand


## https://www.mssqltips.com/sqlservertip/4054/creating-a-date-dimension-or-calendar-table-in-sql-server/


## changes
####Added parameters handling
####Added strip spaces function of TCX files

import os
import argparse
import datetime
from datetime import datetime

## custom modules below
import FileOperations
import ImportFilesIntoDB

## START
print ('Start Program')
print('-'*40)
print (datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

## what arguments do we need?
## source location
## target location
## database server, db name, db credentials?
## delete files after processing?
## full load or incremental load?

parser = argparse.ArgumentParser(description="GPX/Strava File Reader",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)

parser.add_argument("-a", "--archive", action="store_true", help="archive mode")
parser.add_argument("-v", "--verbose", action="store_true", help="increase verbosity")
parser.add_argument("-F", "--Full-load", help="Process all files or only new files; default is new only")
parser.add_argument("--ignore-existing", action="store_true", help="skip files that exist")
parser.add_argument("--exclude", help="files to exclude")
parser.add_argument("SourceFolder", help="Source location for Strava files")
parser.add_argument("TargetFolder", help="Target location for converted TCX to GPX Strava files")
parser.add_argument("DBHost", help="Destination database server - DESKTOP-5QTQUJH\\DEV_INSTANCE is the dev laptop, DESKTOP-A6J6D7Q\\LEROYB_INSTANCE is the desktop")
parser.add_argument("DBName", help="Destination database name")
args = parser.parse_args()
config = vars(args)
args = vars(parser.parse_args())

## print out the arguments passed via the CLI
#print(config)

## first decompress the files in the target folder - need to make this an argument

## what is the source folder containing all the activities? retrieve from the arguments passed to the program and assign to a variable
Source_Folder = args["SourceFolder"]

print(Source_Folder)

## what is the target folder that will receive all the coverted TCX to GPX files? retrieve from the arguments passed to the program and assign to a variable
target_folder = args["TargetFolder"]
#'F:\StravaImport2DB\AllGPX'

print(target_folder)

## here we are looking at gz files all in the format filename.gpx.gz
## the file will be decompressed to a gpx file extension (with same original name)
## the original compressed file will be deleted - although should take that from an argument passed to the proram, maybe we should keep the original file?
## the procedure requires a source folder (containing the compressed files) and a target folder to hold the decompressed files

FileOperations.unGZipTheFiles(Source_Folder, target_folder, "No")
#FileOperations.convertTheFilesFromTCX(target_folder)

# Cleanup TCX files which tend to have rogue spaces at the beginning of the file contents when exporting from Strava
FileOperations.CleanTCXFiles(target_folder)

## import activities overview (file_name, server_name, db_name, schema_name, table_name)
ImportFilesIntoDB.importActivitiesFileIntoDB(Source_Folder + r'\activities.csv', r'DESKTOP-A6J6D7Q\LEROYB_INSTANCE', 'Tracks', 'STG', 'Activities')

## now import all gpx files into staging table
ImportFilesIntoDB.ImportGPXFileIntoDB(target_folder, r'DESKTOP-A6J6D7Q\LEROYB_INSTANCE', 'Tracks', 'STG', 'GPXFiles')

print('-'*40)
print ('End Program')
from datetime import datetime
print (datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
print ('yaba yaba')