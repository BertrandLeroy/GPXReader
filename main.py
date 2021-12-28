
# program used to get Strava files into a SQL server database 
# the database and required tables need to exist beforehand


import datetime
from datetime import datetime

# custom modules below
import FileOperations
import ImportFilesIntoDB

# START
print ('Start Program')
print('-'*40)
print (datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

# first decompress the files in the target folder

target_folder = 'F:\TCXConvert'
#'F:\StravaImport2DB\AllGPX'

# here we are looking at gz files all in the format filename.gpx.gz
# the file will be decompressed to a gpx file extension (with same original name)
# the original compressed file will be deleted

FileOperations.unGZipTheFiles(target_folder)
FileOperations.convertTheFilesFromTCX(target_folder)
# import activities overview

#ImportFilesIntoDB.importActivitiesFileIntoDB(r'F:\StravaImport2DB\activities.csv')

# now import all gpx files into staging table
#ImportFilesIntoDB.ImportGPXFileIntoDB(target_folder)

print('-'*40)
print ('End Program')
from datetime import datetime
print (datetime.now().strftime('%Y-%m-%d %H:%M:%S'))