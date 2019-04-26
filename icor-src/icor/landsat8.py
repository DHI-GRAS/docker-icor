#!/usr/bin/env python

import os
import os.path
import glob
import landsat8_metadata
import math


bandList     = ['B01','B02','B03','B04','B05','B09','B06','B07']


def process_tgz(context, path, dir=os.getcwd()):
    
    context["aot_window_size"] = 500

    list_intermediate_save_file = []

    # ------------------------------------------------------------------------
    # WORKFLOW Landsat8: iCOR
    # ------------------------------------------------------------------------


    l8_metadata = landsat8_metadata.Landsat8_metadata(path)
    l8_metadata.parse_config_file()

    solar_zenith     =  l8_metadata.sun_zenith()
    if solar_zenith == None:
        raise Exception("could not read solar zenith from MTL")
    cos_solar_zenith = math.cos(math.radians(solar_zenith))
    if cos_solar_zenith == 0.0:
        raise Exception("cosine solar zentith should not be zero")
    day_of_year      =  l8_metadata.get_doy()
    if day_of_year == None:
        raise Exception("could not read day of year from MTL")

    #create working folder
    
    input_base_name = path.split("_MTL.txt")[0]
    context["name"] = l8_metadata.get_scene_name()
    context["prefix_input"] = input_base_name.replace("\\","/")
    
    working_folder = os.path.join(dir, context["name"])

    if os.path.isdir(working_folder)==False or os.path.exists(working_folder) == False:
        os.makedirs(working_folder)

   # first save a log of the config
    context.write_config(working_folder)
    
    context["prefix"] = os.path.join(working_folder, context["name"]) 
    
    # set defaults from command line
    aot_string = " --override_aot " + str(context["aot_override"])
    wv_override_string = " --override_watervapor " + str(context["watervapor_override"])
    background_string = " --override_background " + str(context["bg_window"])
    ozone_override_string = " --override_ozone " + str(context["ozone_override"])
   

    # set defaults from command line
    context["max_stages"] = 10
    if context["simec"] == "false":
        context["max_stages"] -= 1 
    
    if context["aot"] == "false":
        context["max_stages"] -= 1 
        
    
    #=========================================================================
    context.enter_stage("Aggregate bands, convert to Geotiff")
    #=========================================================================

    # Create a VRT file (Virtual Dataset)
    #
    # Originally, the bands are delivered in this order: 
    # +=BANDS================================+=Wavelength=(microm)+
    # |Band 1 = Coastal aerosol              | 0.43 to 0.45       |       
    # |Band 2 = Blue                         | 0.45 to 0.51       |
    # |Band 3 = Green                        | 0.53 to 0.59       |
    # |Band 4 = Red                          | 0.64 to 0.67       |
    # |Band 5 = Near Infrared (NIR)          | 0.85 to 0.88       |
    # |Band 6 = SWIR 1                       | 1.57 to 1.65       |
    # |Band 7 = SWIR 2                       | 2.11 to 2.29       |
    # |Band 8 = Panchromatic                 | 0.50 to 0.68       |
    # |Band 9 = Cirrus                       | 1.36 to 1.38       |
    # |Band 10 = Thermal Infrared (TIRS) 1   | 10.60 to 11.19     |
    # |Band 11 = Thermal Infrared (TIRS) 2   | 11.50 to 12.51     |
    # +======================================+====================+
    # 
    # 
    # 
    # For iCOR we reorder the bands 
    # VIS_NIR_SWIR (no suffix)
    # +=BANDS================================+=Wavelength=(microm)+
    # |Band 1 = Coastal aerosol              | 0.43 to 0.45       |       
    # |Band 2 = Blue                         | 0.45 to 0.51       |
    # |Band 3 = Green                        | 0.53 to 0.59       |
    # |Band 4 = Red                          | 0.64 to 0.67       |
    # |Band 5 = Near Infrared (NIR)          | 0.85 to 0.88       |
    # |Band 6 = Cirrus                       | 1.36 to 1.38       |
    # |Band 7 = SWIR 1                       | 1.57 to 1.65       |
    # |Band 8 = SWIR 2                       | 2.11 to 2.29       |
    # +======================================+====================+
    # 
    
    bandlist = [0,1,2,3,4,8,5,6]

    reflectance_list=[]
    radiance_list=[]
    minimum = 0

    reflectance_name=""
    radiance_name=""
    for band in bandlist:
        location = "{prefix_input}_B" + str(band+1) + ".TIF"
        reflectance_name = context["prefix"] + "_ACRUNNER_TOA_Reflectance_"+ str(band) + ".tif"
        radiance_name = context["prefix"] + "_ACRUNNER_TOA_Radiance_"+ str(band)+ ".tif"
    
        context.invoke_ac_runner_mine(
                                "[scale]\n"
                                "scale.input.location=" + location + "\n" +
                                "scale.output.location="+ reflectance_name + "\n" +
                                "scale.gain=" + str(l8_metadata.get_gain_reflectance(band)/cos_solar_zenith)  + "\n" +
                                "scale.offset="+ str(l8_metadata.get_bias_reflectance(band)/cos_solar_zenith) +"\n" +
                                "scale.invalid.minimum=" + str(minimum) + "\n"
                                "scale.zero.invalid=true\n"
                                )

        
        context.invoke_ac_runner_mine(
                                        "[reflectance]\n" +
                                        "reflectance.input.radiance.location=" + reflectance_name + "\n"+
                                        "reflectance.image.dayofyear=94\n"+
                                        "reflectance.bands=0\n"+
                                        "reflectance.lut.bands=" + str(bandlist.index(band)) + "\n"
                                        "reflectance.destination.location="+ radiance_name + "\n"+
                                        "reflectance.override.sza=" + str(l8_metadata.sun_zenith()) + "\n"+
                                        "reflectance.solarirradiance.location={ac_solar_irradiance}\n"+
                                        "reflectance.response.curves.location={ac_response_curves}\n"
                                        "reflectance.invert=true\n"
                                        )

        radiance_list.append(radiance_name)
        reflectance_list.append(reflectance_name)


    
    #=========================================================================
    context.enter_stage("Single to MultiBand Radiance")
    #=========================================================================
    radiance_mb=""
    
    radiance_output_name = context["prefix"] + "_ACRUNNER_Scaled_Radiance.tif"

    for radiance_file in radiance_list:
        radiance_mb +=  radiance_file + " "

    
    context.invoke_ac_runner_mine(
        "[singletomulti fast]\n" + 
        "multiband.input.images=" + radiance_mb + "\n"
        "multiband.output.image=" + radiance_output_name + "\n"
        )

    #=========================================================================
    context.enter_stage("Single to MultiBand Reflectance")
    #=========================================================================
    reflectance_mb=""
    
    reflectance_output_name = context["prefix"] + "_ACRUNNER_TOA_Reflectance.tif"

    for reflectance_file in reflectance_list:
        reflectance_mb +=  reflectance_file + " "

    
    context.invoke_ac_runner_mine(
        "[singletomulti fast]\n" + 
        "multiband.input.images=" + reflectance_mb + "\n"
        "multiband.output.image=" + reflectance_output_name + "\n"
        )



    #=========================================================================
    context.enter_stage("Generate the DEM")
    #=========================================================================


    context.invoke_ac_runner_mine(
                                    "[dem]\n" + 
                                    "dem.reference.location={prefix}_ACRUNNER_TOA_Reflectance.tif\n"
                                    "dem.input.location={dem_world}\n" +
                                    "dem.output.location={prefix}_ACRUNNER_DEM.tif\n"
                                    "dem.conversion.factor=0.001"
                               )

    
    

    #=========================================================================
    context.enter_stage("Cloud Detection")
    #=========================================================================
    low_b = str(context["low_band"])
    
    cloud_low_id_string ="cloud.low.id="+  str(bandList.index(low_b)) + "\n"
    average_threshold_string ="cloud.avg.trh="+  str(context["average_threshold"]) + "\n"
    cloud_low_threshold_string = "cloud.low.trh="+str(context["low_threshold"]) + "\n"

    cirrus_thr = ""
    cirrus_band =""
    if context["cirrus"] == "true" :
        cirrus_thr = "cloud.cirrus.threshold=" + str(context["cirrus_threshold"]) + "\n"
        cirrus_band = "cloud.cirrus.band=5\n"
    
    context.invoke_ac_runner_mine(
        "[cloud detection]\n" +
        "cloud.input.location={prefix}_ACRUNNER_TOA_Reflectance.tif\n" +
         cloud_low_id_string +
        "cloud.high.id=4\n" +
         average_threshold_string +
        cloud_low_threshold_string + 
        "cloud.mask.location={prefix}_ACRUNNER_cloud_mask.tif\n" +
        "cloud.visible.bands= 0 1 2 3\n"+
        cirrus_thr +
        cirrus_band
        )

    wbandid = bandList.index((str(context["water_band"])))
    water_band_string = "water.nir.band=" + str(wbandid) + "\n"
    water_threshold = "water.treshold=" + str(context["water_threshold"]) + "\n"
    #=========================================================================
    context.enter_stage("Water Detection")
    #=========================================================================

    context.invoke_ac_runner_mine(
        "[water detection]\n"
        "water.input.location={prefix}_ACRUNNER_TOA_Reflectance.tif\n"+ 
        water_band_string +
        water_threshold +
        "water.mask.location={prefix}_ACRUNNER_water_mask.tif\n"
        )


    #=========================================================================
    context.enter_stage("Calculate Aerosol Optical Thickness (AOT)")
    #=========================================================================
    
    
    if context["aot"] == "true":
        aot_string = "{prefix}_ACRUNNER_AOT.tif"
        aot_window_string = str(context["aot_window_size"])

        context.invoke_ac_runner_mine(
                        "[aot guanter]\n" 
                        "aot.lut.location={ac_big_disort_lut}\n" 
                        "aot.response.curves.location={ac_response_curves}\n" 
                        "aot.solarirradiance.location={ac_solar_irradiance}\n" 
                        "aot.input.location={prefix}_ACRUNNER_Scaled_Radiance.tif\n" 
                        "aot.output.location=" + aot_string + "\n" 
                        "aot.image.bands=0 1 2 3 4\n" 
                        "aot.image.visible.bands=0 1 2 3\n"  +
                        "aot.square.pixels=" + aot_window_string + "\n" +
                        "aot.ndvi.bands=3 4\n" 
                        "aot.ndvi.list=0.01 0.10 0.45 0.7\n" 
                        "aot.ndvi.refined.bands=3 4\n" 
                        "aot.refpixels.nr=5\n"
                        "aot.limit.refsets=5\n"
                        "aot.weights=2.0 2.0 1.5 1.5 1.0\n" 
                        "aot.centerwl.inverse.location={ac_inverse_profiles}\n" 
                        "aot.vegetation.profiles={ac_vegetation_profiles}\n" 
                        "aot.sand.profiles={ac_soil_profiles}\n" 
                        "aot.watermask.location={prefix}_ACRUNNER_water_mask.tif\n" 
                        "aot.cloudmask.location={prefix}_ACRUNNER_cloud_mask.tif\n" 
                        "aot.cloudmask.dilate=10\n"
                        "aot.override.sza=" + str(l8_metadata.sun_zenith()) + "\n" 
                        "aot.override.vza=" + str(0.0) + "\n" 
                        "aot.override.raa=" + str(0.0) + "\n" +
                        "aot.override.ozone="+ str(context["ozone_override"]) +"\n"
                        "aot.override.watervapor="+str(context["watervapor_override"]) + "\n"
                        "aot.input.elevation.location={prefix}_ACRUNNER_DEM.tif\n"
                        )

                    # check if aot succeeded
        returnCodeAOT = context.invoke_ac_runner_check(
                        "[valid]\n"
                        "valid.input=" + aot_string + "\n"
                        )


    
    
    
    # Run SIMEC
    if context["aot"] == "true" and returnCodeAOT == 0:
        context.add_keep_tmp( str(context["prefix"] ) + "_ACRUNNER_AOT.tif")
        simec_aot_string = "simec.aot.location={prefix}_ACRUNNER_AOT.tif\n"
    else:
        simec_aot_string = "simec.override.aot=" + context["aot_override"] + "\n"


    if context["simec"] == "true":
        #=========================================================================
        context.enter_stage("Use SIMEC to calculate the background")
        #=========================================================================
        background_string="simec.output.location={prefix}_ACRUNNER_SIMEC.tif\n"
        simec_wv_string = "simec.override.watervapor="+ str(context["watervapor_override"]) +"\n" 
        simec_ozone_override_string = "simec.override.ozone="  +str(context["ozone_override"]) + "\n"
        
        context.invoke_ac_runner_mine(
                        "[simec]\n" +
                        "simec.lut.location={ac_big_disort_lut}\n" +
                        "simec.response.curves.location={ac_response_curves}\n" +
                        "simec.radiance.location={prefix}_ACRUNNER_Scaled_Radiance.tif\n" +
                        "simec.subsample.factor=10\n" + 
                        "simec.subsample.band=4\n" +
                        "simec.nir.band=3\n" +
                        "simec.nir780.band=4\n" +
                        "simec.lut.band.nir=3\n" +
                        "simec.lut.band.nir780=4\n" + 
                        "simec.max.window=100\n" +
                        "simec.sensor.resolution_km=0.3\n" +
                        "simec.override.sza=" + str(l8_metadata.sun_zenith()) + "\n" +
                        "simec.override.vza=" + str(0.0) + "\n" +
                        "simec.override.raa=" + str(0.0) + "\n" +
                        "simec.watermask.location={prefix}_ACRUNNER_water_mask.tif\n" +
                        "simec.cloudmask.location={prefix}_ACRUNNER_cloud_mask.tif\n" +
                        "simec.nir.similarity.location={ac_near_similarity_refl}\n" +
                        "simec.elevation.location={prefix}_ACRUNNER_DEM.tif\n" +
                         simec_aot_string +
                         simec_wv_string +
                         simec_ozone_override_string +
                         background_string +
                        "simec.default.background.size=" + str(context["bg_window"]) + "\n"
                        )

        context.add_keep_tmp( context["prefix"] + "_ACRUNNER_SIMEC.tif")



    #=========================================================================
    context.enter_stage("Atmospheric correction")
    #=========================================================================

    atm_background_string = "atm.override.background=" + str(context["bg_window"]) +"\n"
    if context["simec"] == "true":
        atm_background_string = "atm.background.location={prefix}_ACRUNNER_SIMEC.tif\n"

    atm_aot_string = "atm.override.aot=" + str(context["aot_override"]) + "\n"
    if context["aot"] == "true" and returnCodeAOT == 0:
        atm_aot_string = "atm.aot.location={prefix}_ACRUNNER_AOT.tif\n"
        
    atm_ozone_string = "atm.override.ozone=" + str(context["ozone_override"]) + "\n"
    atm_watervapor_string = "atm.override.watervapor=" + str(context["watervapor_override"]) + "\n"


    context.invoke_ac_runner_mine(
                        "[watcor]\n"
                        "atm.lut.location={ac_big_disort_lut}\n" +
                        "atm.radiance.location={prefix}_ACRUNNER_Scaled_Radiance.tif\n" +
                        "atm.override.sza=" + str(l8_metadata.sun_zenith()) + "\n" +
                        "atm.override.vza=" + str(0.0) + "\n" +
                        "atm.override.raa=" + str(0.0) + "\n" +
                        "atm.elevation.location={prefix}_ACRUNNER_DEM.tif\n" +
                        "atm.watermask.location={prefix}_ACRUNNER_water_mask.tif\n" +
                        atm_background_string +
                        atm_aot_string +
                        atm_ozone_string +
                        atm_watervapor_string +
                        "atm.output.location=" +  str(context["output_file"])  + "\n" +
                        "atm.radiance.bands=0 1 2 3 4 6 7\n"
                        )


    #add intermediates    
    
    context.add_keep_tmp( str(context["prefix"] ) + "_ACRUNNER_cloud_mask.tif")
    context.add_keep_tmp( str(context["prefix"] )  +"_ACRUNNER_water_mask.tif")

    #=========================================================================
    context.enter_stage("Remove intermediate files from filesystem")
    #=========================================================================

    keep_tmp = False
    if context["keep_intermediate"] == "true":
        keep_tmp = True
    
        
    context.remove_tmp_files(working_folder,keep_tmp)
    
    print "iCOR Atmospheric correction done for product : " + context["prefix"]