import datetime


class Landsat8_metadata:
    
    def __init__(self,config_file_location):
        self.config_file_location = config_file_location

    def parse_config_file(self):

        self.keys_vars = {}
        with open(self.config_file_location) as config_file:
            for line in config_file:
                name, var = line.partition("=")[::2]
                self.keys_vars[name.strip()] = var


    def sun_elevation(self):
        return float(self.keys_vars["SUN_ELEVATION"])

    def sun_zenith(self):
        return (90.0 - float(self.keys_vars["SUN_ELEVATION"]))

    def get_scene_name(self):
        return str(self.keys_vars["LANDSAT_SCENE_ID"]).strip("\n").strip(" ").replace('"','')



    def get_gain_reflectance(self,band):
        return float(self.keys_vars["REFLECTANCE_MULT_BAND_"+ str(band+1)])
    
        
    def get_bias_reflectance(self,band):
        return float(self.keys_vars["REFLECTANCE_ADD_BAND_"+ str(band+1)])

    def get_earth_sun_distance(self):
        #Get the distance in Astronomical Units (1AU = 149,599,650 km)
        sun_earth_distance = 1.0 - 0.01672*cos(0.9852*(get_doy() - 4.0)*(deg_to_rad))
        return sun_earth_distance



    def get_doy(self):

        date_mtl = self.keys_vars["DATE_ACQUIRED"]
        
        if date_mtl == None:
            raise Exception("Could not parse DATA_ACQUIRED from MTL file")
        date_mtl = date_mtl.replace("\n","")
        date_mtl = date_mtl.replace(" ","")
        date_mtl = date_mtl.strip()
        date = datetime.datetime.strptime(date_mtl,"%Y-%m-%d")
        doy =  date.timetuple().tm_yday
        return doy