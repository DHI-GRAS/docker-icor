
import os
import xml.etree.cElementTree as cET 

import datetime
import platform

TIMEFORMAT="%Y-%m-%dT%H:%M:%S.%fZ"


SENTINEL2_BANDCOUNT = 13

resolution_list = [60,10,10,10,20,20,20,10,20,60,60,20,20]


class BandData:
    
    def __init__(self,id,name):
        self.name = name
        self.id = id

    def set_location(self,location):
        self.location = location

    def set_reflectance_location(self,reflectance_location):
        self.reflectance_location = reflectance_location

    def set_resolution(self,resolution):
        self.resolution = resolution

    def set_gain(self,gain):
        self.gain = gain

    def set_offset(self,offset):
        self.offset = offset

    def get_gain(self):
        return self.gain

    def get_offset(self):
        return self.offset

    def get_id(self):
        return self.id



class Granule:
    
    def __init__(self, config_file):
        self.config_file = config_file
        self.band_list=[]

    def set_mean_solar_zenith(self,mean_solar_zenith):
        self.mean_solar_zenith=mean_solar_zenith

    def set_mean_solar_azimuth(self,mean_solar_azimuth):
        self.mean_solar_azimuth=mean_solar_azimuth

    def set_mean_view_azimuth(self,mean_view_azimuth):
        self.mean_view_azimuth=mean_view_azimuth

    def set_mean_view_zenith(self,mean_view_zenith):
        self.mean_view_zenith = mean_view_zenith

    def append_band(self,band):
        self.band_list.append(band)
    
    def set_base_name(self,base_name):
        self.base_name=base_name


    def set_mean_relative_azimuth(self):
            raa = (self.mean_view_azimuth - self.mean_solar_azimuth)
            if raa < -180.0:
                raa+=360.0
            if raa > 180.0:
                raa-=360.0        
            
            self.mean_relative_azimuth = abs(raa)
    def set_output_folder(self,folder):
        self.output_folder = folder

    def get_output_folder(self):
        return self.output_folder

    def parse_config_file(self):
 
        try:   
 
            output_folder = os.path.dirname(self.config_file)

            self.set_output_folder(output_folder)

            if platform.system() == "Windows" and len(self.config_file) > 260:
                raise Exception("pathlength is too long - max allowed on windows platforms is 260.")

            tree = cET.parse(self.config_file).getroot()

            ns = tree.tag.split('}')[0].strip('{')

            geo  = tree.find("{%s}Geometric_Info"%ns)

            sun_angels = geo.find("Tile_Angles").find("Sun_Angles_Grid")

            zenith = sun_angels.find("Zenith")
            solar_zenith = zenith.find("Values_List").findall("VALUES")

            zenith_values_list=[]
    
            for zenith_vals in solar_zenith:
                vals = zenith_vals.text.split()
                for val in vals:
                    zenith_values_list.append(float(val))
            
        
        
            azimuth = sun_angels.find("Azimuth")
            solar_azimuth = azimuth.find("Values_List").findall("VALUES")
 
            azimuth_values_list=[]
            for azimuth_vals in solar_azimuth:
                vals = azimuth_vals.text.split()
                for val in vals:
                    azimuth_values_list.append(float(val))
            
        


            mean_viewing_angles = geo.find("Tile_Angles").find("Mean_Viewing_Incidence_Angle_List").findall("Mean_Viewing_Incidence_Angle")
        
            view_zenith  = 0.0
            view_azimuth = 0.0
            for mean_viewing_angle in mean_viewing_angles:
                view_zenith += float(mean_viewing_angle.find("ZENITH_ANGLE").text)
                view_azimuth +=  float(mean_viewing_angle.find("AZIMUTH_ANGLE").text)


            mean_solar_zenith  = sum(zenith_values_list)/float(len(zenith_values_list))
            mean_solar_azimuth = sum(azimuth_values_list)/float(len(azimuth_values_list))
            mean_view_zenith   = view_zenith / float(len(mean_viewing_angles))
            mean_view_azimuth  = view_azimuth / float(len(mean_viewing_angles))


            self.set_mean_solar_zenith(mean_solar_zenith)
            self.set_mean_solar_azimuth(mean_solar_zenith)
            self.set_mean_view_zenith(mean_view_zenith)
            self.set_mean_view_azimuth(mean_view_azimuth)
            self.set_mean_relative_azimuth()
        
        
        

        except Exception, e:
            raise Exception("Something went wrong while parsing the S2 metadata file: %s" % str(e)) 




def check_and_update_filename(input_file):

    if str(input_file).find("GRANULE"):
        d = os.path.dirname(input_file)
        d = os.path.dirname(d)
        d = os.path.dirname(d)
        # root folder 
        d = os.path.dirname(d)
        
        for file in os.listdir(d):
            if file.endswith(".xml"):
                if "L1C" in str(file) :
                    return os.path.join(str(d) , str(file))
                if "L2A" in str(file) :
                    return os.path.join(str(d) , str(file))

        
    else:
        return input_file




def parse(input_file,bandList):
    
#+-BANDS----------------------------+-Wavelength-(um)----+-Resolution-(m)---+
#|0  BAND1 - Aerosols               |         0.443      |       60         |
#|1  BAND2 - Blue                   |         0.490      |       10         |
#|2  BAND3 - Green                  |         0.560      |       10         |
#|3  BAND4 - Red                    |         0.665      |       10         |
#|4  BAND5 - narrow1 (red-edge)     |         0.705      |       20         |
#|5  BAND6 - narrow2 (red-edge)     |         0.740      |       20         |
#|6  BAND7 - narrow3 (red-edge)     |         0.783      |       20         |
#|7  BAND8 - NIR                    |         0.842      |       10         |
#|8  BAND8b- narrow4 (red-edge)     |         0.865      |       20         |
#|9  BAND9 - Water Vapour           |         0.945      |       60         |
#|10 BAND10- Cirrus                 |         1.380      |       60         |
#|11 BAND11- SWIR1                  |         1.610      |       20         |
#|12 BAND12- SWIR2                  |         2.190      |       20         |
#+----------------------------------+--------------------+------------------+
    
    try: 

        
        input_file = check_and_update_filename(input_file)
        

        tree = cET.parse(input_file).getroot()
        #get namespace for this file
        ns = tree.tag.split('}')[0].strip('{')

        t = tree.find('{%s}General_Info'%ns)

        startTimeStr = t.find("Product_Info").find("Datatake").find("DATATAKE_SENSING_START").text

        startTime = datetime.datetime.strptime(startTimeStr,TIMEFORMAT)
        
        year    = startTime.year
        month   = startTime.month
        day     = startTime.day
        hour    = startTime.hour
        minute  = startTime.minute
        second  = startTime.second




        if bandList != None:
            bandCount = len(bandList)
        else:
            raise Exception("bandList should not be None")


        # these are the TILES locations
        tiles = t.find("Product_Info").find("Product_Organisation").findall("Granule_List")
        #spectralinfo = t.find("Product_Image_Characteristics").find("Spectral_Information_List").findall("Spectral_Information")
        
        doy_scale = 1.0 #float(t.find("Product_Image_Characteristics").find("Reflectance_Conversion").find("U").text)
        
        solar_irradiance = t.find("Product_Image_Characteristics").find("Reflectance_Conversion").find("Solar_Irradiance_List").findall("SOLAR_IRRADIANCE")

        quantification_value = float(t.find("Product_Image_Characteristics").find("QUANTIFICATION_VALUE").text)

        path = os.path.dirname(input_file)

        
        granules_location_prefix = "GRANULE/"


        tiles_data = []
        for tile in tiles:
            
            granule_xml = tile.find("Granules")
            if granule_xml == None:
                granule_xml = tile.find("Granule") #the latest version :((((

            granule_identifier = granule_xml.get("granuleIdentifier")
            granule_location = granules_location_prefix + granule_identifier
            granule_path = path+"/" + granule_location

            if os.path.exists(granule_path) == False:
                dirs = os.listdir(path +"/"+ granules_location_prefix)
                if len(dirs) == 1:
                    granule_path = path+"/" + granules_location_prefix + dirs[0]
                else:
                    raise Exception("Can't find granule location %s" % str(granule_location))


            xml = filter(lambda x: x.endswith('.xml'), os.listdir(granule_path))
            granule_config_xml = granule_path + "/" + xml[0]
            
            granule = Granule(granule_config_xml)
            granule.set_base_name(granule_identifier)
            granule.parse_config_file()
    
            granule_location = path + "/"

            
            im_id = granule_xml.findall("IMAGE_FILE")
            if len(im_id) == 0:
                im_id = granule_xml.findall("IMAGE_ID")
                granule_location = path + "/GRANULE/" + granule_identifier + "/IMG_DATA/" 
            

            if len(im_id) == 0:
                raise Exception("Could not retrieve file locations form the product xml %s" % input_file)
            

            for i in range(0,bandCount):
                name = bandList[i]
                location =""
                for loc in im_id:
                    name_file = loc.text[-3:]
                    if name_file == name:
                        location = loc.text
                
                band = BandData(i,name)
                band.set_gain(1.0/(quantification_value*doy_scale))
                band.set_offset(0.0)
                band.set_location(granule_location+location+".jp2")
                #band.set_resolution(float(spectralinfo[i].find("RESOLUTION").text))
                band.set_resolution(float(resolution_list[i]))
                
                granule.append_band(band)
        
            tiles_data.append(granule)



    except Exception, e:
        raise Exception("Something went wrong while parsing the S2 metadata file: %s" % str(e)) 


    
    return tiles_data
