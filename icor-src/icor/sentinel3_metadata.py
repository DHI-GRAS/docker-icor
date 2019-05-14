import os
import xml.etree.cElementTree as cET 

import time
import platform
import math
import csv




class S3_metadata:
     
    def __init__(self):
         self.distance_correction=1.0
         self.productname = "dummy"


    def set_distance_correction(self,distance_correction):
        self.distance_correction = distance_correction

    def get_distance_correction(self):
        return self.distance_correction

    def set_productname(self,productname):
        self.productname=productname
    def get_productname(self):
        return self.productname



TIMEFORMAT="%Y-%m-%dT%H:%M:%S.%fZ"

bands =["01","02","03","04","05","06","07","08","09","10","11","12","13","14","15","16","17","18","19","20","21"]

bands_final = ["0","01","02","03","04","05","06","07","08","09","10","11","15","16","17","20"]

snap_bands =["B01","B02","B03","B04","B05","B06","B07","B08","B09","B10","B11","B12","B13","B14","B15","B16","B17","B18","B19","B20","B21"]


svc_default =  [0.9798,0.9718,0.9747,0.9781,0.9827,0.9892,0.9922,0.992,0.9943,0.9962,0.996,1.003,1.0,1.0,1.0,1.005,1,0.996,1.0,1.0,0.914]


icor_dir = str(os.environ['ICOR_DIR'])


SENTINEL3_BANDCOUNT = 21


def sun_earth_distance(doy):
    return (1.0 - 0.01672*math.cos(0.9852*(doy - 4.0)*(math.pi/180.0)))



def get_band_id(name):

    
    try:
      id = snap_bands.index(str(name))
      return id
      
    except ValueError:
        print "cannot find bandname in list for snap bands"



    
    

def parse(input_file):
    try: 

                
        tree = cET.parse(input_file).getroot()
        #get namespace for this file

        ns = tree.tag.split('}')[1].strip('{')

        ns_safe = "http://www.esa.int/safe/sentinel/1.1"
        instrument_name="http://www.esa.int/safe/sentinel/sentinel-3/1.0" 

        correction =1.0
        productname = "dummy"
        t = tree.find('metadataSection')
        period = object
        for element in t:
            if str(element.attrib).count("acquisitionPeriod") > 0:
                period = element
                time_date = period.find('metadataWrap').find('xmlData').find("{%s}acquisitionPeriod"%ns_safe).find("{%s}startTime"%ns_safe)
                startTimeStr = time_date.text
                print "image date/time stamp :: " + startTimeStr
                startTime = time.strptime(startTimeStr,TIMEFORMAT)
                correction = sun_earth_distance(startTime.tm_yday)

            elif str(element.attrib).count("generalProductInformation") > 0:
                period = element
                
                prod_name = period.find('metadataWrap').find('xmlData').find("{%s}generalProductInformation"%instrument_name).find("{%s}productName"%instrument_name)
                productname = prod_name.text
                productname = productname.replace(".","_")

    except Exception, e:
        raise Exception("Something went wrong while parsing the S3 metadata file: %s" % str(e)) 

    metadata = S3_metadata()

    metadata.set_distance_correction(correction)
    metadata.set_productname(str(productname))

    
    return metadata

    
def get_svc(sensorname):


    print "loading system vicarious gain file ..."

    svc_list = []

    filelocation = icor_dir + "/bin/Sensor_Sentinel3/s3_svc/S" + sensorname + ".csv"

    try: 
        with open(filelocation) as csvfile:

            r_file = csv.reader(csvfile,delimiter=',')
            for line in r_file:
                for value in line:
                    svc_list.append(float(value))
            
    
    except Exception, e:
        raise Exception("Something went wrong while parsing the S3 svc gains file: %s" % str(e))             
             
    
    return svc_list




    

