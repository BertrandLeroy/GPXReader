

import os, zipfile
import gzip
import fnmatch
from tcx2gpx import TCX2GPX

def unZipTheFiles(dir_name):
    # https://stackoverflow.com/questions/31346790/unzip-all-zipped-files-in-a-folder-to-that-same-folder-using-python-2-7-5
    #dir_name = 'C:\\SomeDirectory'
    extension = '.zip'

    os.chdir(dir_name) # change directory from working dir to dir with files

    for item in os.listdir(dir_name): # loop through items in dir
        if item.endswith(extension): # check for ".zip" extension
            file_name = os.path.abspath(item) # get full path of files
            zip_ref = zipfile.ZipFile(file_name) # create zipfile object
            zip_ref.extractall(dir_name) # extract file to dir
            zip_ref.close() # close file
            os.remove(file_name) # delete zipped file

#def gunzip(file_path,output_path):
#    with gzip.open(file_path,"rb") as f_in, open(output_path,"wb") as f_out:
#        shutil.copyfileobj(f_in, f_out)

def unGZipTheFiles(dir_name):
    #https://www.tutorialspoint.com/python-support-for-gzip-files-gzip
    extension = '.gz'

    os.chdir(dir_name) # change directory from working dir to dir with files

    for item in os.listdir(dir_name): # loop through items in dir
        if item.endswith(extension): # check for ".zip" extension
            file_name = os.path.abspath(item) # get full path of files
            input = gzip.GzipFile(file_name, 'rb')
            s = input.read()
            input.close()

            output = open(os.path.join(dir_name, os.path.splitext(item)[0]), 'wb')
            output.write(s)
            output.close()

            os.remove(file_name) # delete zipped file

def convertTheFilesFromTCX(dir_name):
    # https://stackoverflow.com/questions/31346790/unzip-all-zipped-files-in-a-folder-to-that-same-folder-using-python-2-7-5
    #dir_name = 'C:\\SomeDirectory'
    extension = '.tcx'

    os.chdir(dir_name) # change directory from working dir to dir with files

    for item in os.listdir(dir_name): # loop through items in dir
        if item.endswith(extension): # check for ".zip" extension
            file_name = os.path.abspath(item) # get full path of files
            #zip_ref = zipfile.ZipFile(file_name) # create zipfile object
            #zip_ref.extractall(dir_name) # extract file to dir
            #zip_ref.close() # close file
            #os.remove(file_name) # delete zipped file

            gps_object = tcx2gpx.TCX2GPX(tcx_path=file_name)
            gps_object.convert()



#def gunzip(file_path,output_path):
#    with gzip.open(file_path,"rb") as f_in, open(output_path,"wb") as f_out:
#        shutil.copyfileobj(f_in, f_out)




#    walker = os.walk(dir_name)
#    for root,dirs,files in walker:
#        for f in files:
#            if fnmatch.fnmatch(f,"*.gz"):
#                gunzip(f,f.replace(".gz",""))
