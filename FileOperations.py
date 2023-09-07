

import os, zipfile
import gzip
import fnmatch
#from tcx2gpx import TCX2GPX

import logging
from datetime import datetime
from pathlib import Path

from gpxpy import gpx
from tcxparser import TCXParser
import dateutil.parser

LOGGER = logging.getLogger('tcx2gpx')


class TCX2GPX():
    """
    Convert tcx files to gpx.
    """

    def __init__(self, tcx_path, outdir=None):
        """
        Initialise the class.
        """
        self.tcx_path = Path(tcx_path)
        self.outdir = outdir
        self.tcx = None
        self.track_points = None
        self.gpx = gpx.GPX()

    def convert(self):
        """
        Convert tcx to gpx.
        """
        self.read_tcx()
        self.extract_track_points()
        self.create_gpx()
        self.write_gpx()

    def read_tcx(self):
        """
        Read a TCX file.

        Parameter
        ---------
        tcx_path: str
            Valid path to a TCX file.
        """
        try:
            self.tcx = TCXParser(str(self.tcx_path.resolve()))
            LOGGER.info(
                'Reading                     : {}'.format(self.tcx_path))
        except TypeError as not_pathlib:
            print(not_pathlib)
            raise TypeError('File path did not resolve.')

    def extract_track_points(self):
        """
        Extract and combine features from tcx
        """
        self.track_points = zip(self.tcx.position_values(),
                                self.tcx.altitude_points(),
                                self.tcx.time_values())
        LOGGER.info('Extracting track points from : {}'.format(self.tcx_path))

    def create_gpx(self):
        """
        Create GPX object.
        """
        self.gpx.name = dateutil.parser.parse(
            self.tcx.started_at).strftime('%Y-%m-%d %H:%M:%S')
        self.gpx.description = ''
        gpx_track = gpx.GPXTrack(name=dateutil.parser.parse(self.tcx.started_at).strftime('%Y-%m-%d %H:%M:%S'),
                                 description='')
        gpx_track.type = self.tcx.activity_type
        # gpx_track.extensions = '<topografix:color>c0c0c0</topografix:color>'
        self.gpx.tracks.append(gpx_track)
        gpx_segment = gpx.GPXTrackSegment()
        gpx_track.segments.append(gpx_segment)
        for track_point in self.track_points:
            gpx_trackpoint = gpx.GPXTrackPoint(latitude=track_point[0][0],
                                               longitude=track_point[0][1],
                                               elevation=track_point[1],
                                               time=datetime.strptime(track_point[2],
                                                                      '%Y-%m-%dT%H:%M:%SZ'))
            gpx_segment.points.append(gpx_trackpoint)
        LOGGER.info('Creating GPX for             : {}'.format(self.tcx_path))

    def write_gpx(self):
        """
        Write GPX object to file.
        """
        out = str(self.tcx_path.resolve()).replace('.tcx', '.gpx')
        with open(out, 'w') as output:
            output.write(self.gpx.to_xml())
        LOGGER.info('GPX written to               : {}'.format(out))


def unZipTheFiles(Source_Folder, target_folder, DeleteSwitch):
    # https://stackoverflow.com/questions/31346790/unzip-all-zipped-files-in-a-folder-to-that-same-folder-using-python-2-7-5
    #dir_name = 'C:\\SomeDirectory'
    extension = '.zip'

    os.chdir(Source_Folder) # change directory from working dir to dir with files

    for item in os.listdir(Source_Folder): # loop through items in dir
        if item.endswith(extension): # check for ".zip" extension
            file_name = os.path.abspath(item) # get full path of files
            zip_ref = zipfile.ZipFile(file_name) # create zipfile object
            zip_ref.extractall(target_folder) # extract file to dir
            zip_ref.close() # close file
            if DeleteSwitch == "Yes":
                os.remove(file_name) # delete zipped file

#def gunzip(file_path,output_path):
#    with gzip.open(file_path,"rb") as f_in, open(output_path,"wb") as f_out:
#        shutil.copyfileobj(f_in, f_out)

def unGZipTheFiles(Source_Folder, target_folder, DeleteSwitch):
    #https://www.tutorialspoint.com/python-support-for-gzip-files-gzip
    extension = '.gz'

    os.chdir(Source_Folder) # change directory from working dir to dir with files

    for item in os.listdir(Source_Folder): # loop through items in dir
        if item.endswith(extension): # check for ".zip" extension
            file_name = os.path.abspath(item) # get full path of files
            input = gzip.GzipFile(file_name, 'rb')
            s = input.read()
            input.close()

            output = open(os.path.join(target_folder, os.path.splitext(item)[0]), 'wb')
            output.write(s)
            output.close()
            
            if DeleteSwitch == "Yes":
                os.remove(file_name) # delete zipped file


def CleanTCXFiles(dir_name):
    # https://stackoverflow.com/questions/31346790/unzip-all-zipped-files-in-a-folder-to-that-same-folder-using-python-2-7-5
    #dir_name = 'C:\\SomeDirectory'
    extension = '.tcx'

    os.chdir(dir_name) # change directory from working dir to dir with files

    for item in os.listdir(dir_name): # loop through items in dir
        if item.endswith(extension): # check for ".zip" extension
            file_name = os.path.abspath(item) # get full path of files
            
            file_contents = Path(file_name).read_text()
            
            file_contents = file_contents.strip()
            
            with open(file_name, "w") as text_file:
                print(file_contents, file=text_file)
                
                
def convertTheFilesFromTCX(dir_name):
    # https://stackoverflow.com/questions/31346790/unzip-all-zipped-files-in-a-folder-to-that-same-folder-using-python-2-7-5
    #dir_name = 'C:\\SomeDirectory'
    extension = '.tcx'

    os.chdir(dir_name) # change directory from working dir to dir with files

    for item in os.listdir(dir_name): # loop through items in dir
        if item.endswith(extension): # check for ".zip" extension
            file_name = os.path.abspath(item) # get full path of files
            
            file_contents = Path(file_name).read_text()
            
            file_contents = file_contents.strip()
            
            with open(file_name, "w") as text_file:
                print(file_contents, file=text_file)
            #zip_ref = zipfile.ZipFile(file_name) # create zipfile object
            #zip_ref.extractall(dir_name) # extract file to dir
            #zip_ref.close() # close file
            #os.remove(file_name) # delete zipped file

            ## first need to trim the file contents for each tcx files, because these are malformed
            #file_object = file_name.open()
            #file_contents = file_object.read()
            #file_object.write(file_contents.strip())
            #file_object.close()
            
            gps_object = TCX2GPX(tcx_path=file_name)
            gps_object.convert()



#def gunzip(file_path,output_path):
#    with gzip.open(file_path,"rb") as f_in, open(output_path,"wb") as f_out:
#        shutil.copyfileobj(f_in, f_out)




#    walker = os.walk(dir_name)
#    for root,dirs,files in walker:
#        for f in files:
#            if fnmatch.fnmatch(f,"*.gz"):
#                gunzip(f,f.replace(".gz",""))
