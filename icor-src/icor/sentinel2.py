#!/usr/bin/env python

import os
import os.path
import glob
import threading

import context

import sentinel2_metadata

bandList     = ['B01','B02','B03','B04','B05','B06','B07','B08','B8A','B09','B10','B11','B12']
bandList_10m = ['B02','B03','B04','B08']
bandList_20m = ['B05','B06','B07','B8A','B11','B12']
bandList_60m = ['B01','B09','B10']



# ================================================== 
# Sentinel2 Specs                                    
# ================================================== 
# all 
# +=BANDS==============================+=Wavelength-(um)====+=Resolution-(m)===+ 
# | (0) BAND1 - Aerosols               |         0.443      |       60         | 
# | (1) BAND2 - Blue                   |         0.490      |       10         | 
# | (2) BAND3 - Green                  |         0.560      |       10         | 
# | (3) BAND4 - Red                    |         0.665      |       10         | 
# | (4) BAND5 - narrow1 (red-edge)     |         0.705      |       20         | 
# | (5) BAND6 - narrow2 (red-edge)     |         0.740      |       20         | 
# | (6) BAND7 - narrow3 (red-edge)     |         0.783      |       20         | 
# | (7) BAND8 - NIR                    |         0.842      |       10         | 
# | (8) BAND8b- narrow4 (red-edge)     |         0.865      |       20         | 
# | (9) BAND9 - Water Vapour           |         0.945      |       60         | 
# | (10)BAND10- Cirrus                 |         1.380      |       60         | 
# | (11)BAND11- SWIR1                  |         1.610      |       20         | 
# | (12)BAND12- SWIR2                  |         2.190      |       20         | 
# +====================================+====================+==================+ 

# 10M bands 
# +==BAND=#====+==ORIGNAL=BAND==+==CW=(nm)===+==SW=(nm)===+==RES=(m)===+ 
# |  (0)1         |  B02           |  490       |  65        |  10        | 
# |  (1)2         |  B03           |  560       |  35        |  10        | 
# |  (2)3         |  B04           |  665       |  30        |  10        | 
# |  (3)4         |  B08           |  842       |  115       |  10        | 
# +============+================+============+============+============+ 

# 20M bands 
# +==BAND=#====+==ORIGNAL=BAND==+==CW=(nm)===+==SW=(nm)===+==RES=(m)===+ 
# |  (0)1         |  B05           |  705       |  15        |  20        | 
# |  (1)2         |  B06           |  740       |  15        |  20        | 
# |  (2)3         |  B07           |  783       |  20        |  20        | 
# |  (3)4         |  B8A           |  865       |  20        |  20        | 
# |  (4)5         |  B11           |  1610      |  90        |  20        | 
# |  (5)6         |  B12           |  2190      |  180       |  20        | 
# +============+================+============+============+============+ 

# 60M bands 
# +==BAND=#====+==ORIGNAL=BAND==+==CW=(nm)===+==SW=(nm)===+==RES=(m)===+ 
# |  (0)1         |  B01           |  443       |  20        |  60     | 
# |  (1)2         |  B09           |  945       |  20        |  60     | 
# |  (2)3         |  B10           |  1375      |  30        |  60     | 
# +============+================+============+============+============+ 


def process_saf(context, path, dir=os.getcwd()):

    context["aot_window_size"] = 250

    #
    # find the tiles for this Sentinel 2 product. The French, they call it "Granules"          
    #
    granules = sentinel2_metadata.parse(path,bandList)
    

    #-------------------------------------------------------------------------
    # WORKFLOW Sentinel2: OPERA
    #-------------------------------------------------------------------------

    thread_list = []
    for granule in granules:
        print "create thread for granule " + granule.base_name
        thread_context = context.copy_self()
        t = threading.Thread(target=sentinel2_granule,args=(granule,thread_context,dir))
        t.start()
        t.join()        

        #thread_list.append(t)


def sentinel2_granule( granule,thread_context,dir):



    # set defaults from command line
    thread_context["max_stages"] = 25
    if thread_context["aot"] == "false":
        thread_context["max_stages"] -= 3
 
    if thread_context["simec"] == "false":
        thread_context["max_stages"] -= 5
    
    if thread_context["watervapor"] == "false" :
        thread_context["max_stages"] -= 3
 
    if thread_context["keep_intermediate"] == "false" :
        thread_context["max_stages"] -= 1
    


    input_base_name = granule.base_name.replace(".","_")
    print input_base_name
    thread_context["name"] = input_base_name
    thread_context["prefix_input"] = input_base_name.replace("\\","/")
    granule_working_dir = os.path.join(dir, thread_context["name"])
    thread_context["prefix"] = granule_working_dir + "/" + input_base_name
    
    
    if os.path.isdir(granule_working_dir)==False or os.path.exists(granule_working_dir) == False:
        os.makedirs(granule_working_dir)

    thread_context.write_config(granule_working_dir)

    # radiance and reflectance filenames
    # 60m
    radiance_60m = dict()    
    reflectance_60m = dict()
    # 20m    
    radiance_20m = dict()
    reflectance_20m = dict()
    # 10m
    radiance_10m = dict()    
    reflectance_10m = dict()

    
    dem_list=dict()
    

    
    #=========================================================================
    thread_context.enter_stage("Convert to scaled radiance")
    #=========================================================================


    for band in granule.band_list:
        
        reflectance_name = thread_context["prefix"] + "_ACRUNNER_Scaled_Reflectance_"+ band.name + ".tif"
        radiance_name = thread_context["prefix"] + "_ACRUNNER_TOA_Radiance_"+ band.name + ".tif"

        minimum = 5
        if band.name=="B10":
             minimum=0

        thread_context.invoke_ac_runner_mine(
                                "[scale]\n"
                                "scale.input.location=" + band.location + "\n" +
                                "scale.output.location="+ reflectance_name + "\n" +
                                "scale.gain=" + str(band.get_gain())+ "\n" +
                                "scale.offset="+ str(band.get_offset())+"\n" +
                                "scale.invalid.minimum=" + str(minimum) + "\n"
                                "scale.zero.invalid=true\n"
                                )



        
        thread_context.invoke_ac_runner_mine(
                                        "[reflectance]\n" +
                                        "reflectance.input.radiance.location=" + reflectance_name + "\n"+
                                        "reflectance.image.dayofyear=94\n"+
                                        "reflectance.bands=0\n"+
                                        "reflectance.lut.bands=" + str(band.get_id()) + "\n"
                                        "reflectance.destination.location="+ radiance_name + "\n"+
                                        "reflectance.override.sza=" + str(granule.mean_solar_zenith) + "\n"+
                                        "reflectance.solarirradiance.location={ac_solar_irradiance}\n"+
                                        "reflectance.response.curves.location={ac_response_curves_all}\n"
                                        "reflectance.invert=true\n"
                                        )



        if band.resolution == 60:
            reflectance_60m[band.name] = reflectance_name
            radiance_60m[band.name]    = radiance_name
        if band.resolution == 20:
            reflectance_20m[band.name] = reflectance_name
            radiance_20m[band.name]    = radiance_name
        if band.resolution == 10:
            reflectance_10m[band.name] = reflectance_name
            radiance_10m[band.name]    = radiance_name




    #=========================================================================
    thread_context.enter_stage("Generate DEM")
    #=========================================================================

    # Generate a DEM that matches the size of the input images.  A DEM will be
    # generated for the 10M, 20M and 60M bands.
    dem_list['60']= thread_context["prefix"] +"_DEM_60M.tif"
    thread_context.invoke_ac_runner_mine(
                                    "[dem]\n" + 
                                    "dem.reference.location=" + reflectance_60m['B01'] + "\n"
                                    "dem.input.location={dem_world}\n" +
                                    "dem.output.location=" + thread_context["prefix"] +"_DEM_60M.tif\n"
                                    "dem.conversion.factor=0.001"
                                )

    dem_list['20']= thread_context["prefix"] +"_DEM_20M.tif"
    thread_context.invoke_ac_runner_mine(
                                    "[dem]\n" + 
                                    "dem.reference.location=" + reflectance_20m['B05'] + "\n"
                                    "dem.input.location={dem_world}\n" +
                                    "dem.output.location=" + thread_context["prefix"] +"_DEM_20M.tif\n"
                                    "dem.conversion.factor=0.001"
                                )



    dem_list['10']= thread_context["prefix"] +"_DEM_10M.tif"
    thread_context.invoke_ac_runner_mine(
                                    "[dem]\n" + 
                                    "dem.reference.location=" + reflectance_10m['B02'] + "\n"
                                    "dem.input.location={dem_world}\n" +
                                    "dem.output.location=" + thread_context["prefix"] +"_DEM_10M.tif\n"
                                    "dem.conversion.factor=0.001"
                               )
    
    
    
    #=========================================================================
    thread_context.enter_stage("Resize images to 60m")
    #=========================================================================
    # 20 -> 60    
 
    reflectance_60M_ALL = reflectance_60m
    radiance_60M_ALL    = radiance_60m

    # reflectances
    input_location_list=""
    output_location_list=""
    for key in reflectance_20m.keys():
        input_location_list  = input_location_list + reflectance_20m[key] + " "
        output_location = thread_context["prefix"]+ "_Reflectance_"+ key +"_60M.tif "
        reflectance_60M_ALL[key] = output_location
        output_location_list += output_location 

    
    for key in reflectance_10m.keys():
        input_location_list = input_location_list + reflectance_10m[key] + " "
        output_location = thread_context["prefix"]+ "_Reflectance_" + key +"_60M.tif "
        reflectance_60M_ALL[key] = output_location
        output_location_list += output_location


    for key in radiance_20m.keys():
        input_location_list = input_location_list + radiance_20m[key] + " "
        output_location = thread_context["prefix"]+ "_Radiance_"+ key +"_60M.tif "
        radiance_60M_ALL[key] = output_location
        output_location_list += output_location



    for key in radiance_10m.keys():
        input_location_list = input_location_list + radiance_10m[key] + " "
        output_location = thread_context["prefix"]+ "_Radiance_"+ key +"_60M.tif "
        radiance_60M_ALL[key] = output_location
        output_location_list += output_location


    thread_context.invoke_ac_runner_mine(
                                    "[resize nearest]\n" + 
                                    "resize.image.location=" + input_location_list + "\n"+
                                    "resize.reference.location="+ reflectance_60m['B01'] +"\n" +
                                    "resize.destination.location=" + output_location_list +  "\n"
                                 )

    #=========================================================================
    thread_context.enter_stage("Single to MultiBand Radiance - 60M ALL")
    #=========================================================================
    radiance_mb=""
    
    radiance_output_multiband_60M_ALL =  thread_context["prefix"] + "_Radiance_60M_ALL.tif"

    for name in bandList:
        radiance_mb +=  radiance_60M_ALL[name] + " "

    
    thread_context.invoke_ac_runner_mine(
        "[singletomulti fast]\n" + 
        "multiband.input.images=" + radiance_mb + "\n"
        "multiband.output.image=" + radiance_output_multiband_60M_ALL + "\n"
        )


    #=========================================================================
    thread_context.enter_stage("Single to MultiBand Radiance - 20M")
    #=========================================================================
    radiance_mb=""
    
    radiance_output_multiband_20M =  thread_context["prefix"] + "_Radiance_20M_ALL.tif"

    for name in bandList_20m:
        radiance_mb +=  radiance_20m[name] + " "

    
    thread_context.invoke_ac_runner_mine(
        "[singletomulti fast]\n" + 
        "multiband.input.images=" + radiance_mb + "\n"
        "multiband.output.image=" + radiance_output_multiband_20M + "\n"
        )


    #=========================================================================
    thread_context.enter_stage("Single to MultiBand Radiance - 10M")
    #=========================================================================
    radiance_mb=""
    
    radiance_output_multiband_10M =  thread_context["prefix"] + "_Radiance_10M_ALL.tif"

    for name in bandList_10m:
        radiance_mb +=  radiance_10m[name] + " "

    
    thread_context.invoke_ac_runner_mine(
        "[singletomulti fast]\n" + 
        "multiband.input.images=" + radiance_mb + "\n"
        "multiband.output.image=" + radiance_output_multiband_10M + "\n"
        )


    #=========================================================================
    thread_context.enter_stage("Single to MultiBand Reflectance - 60M ALL")
    #=========================================================================
    reflectance_mb=""
    
    reflectance_output_multiband_60M_ALL =  thread_context["prefix"] + "_Reflectance_60M_ALL.tif"

    for name in bandList:
        reflectance_mb +=  reflectance_60M_ALL[name] + " "

    
    thread_context.invoke_ac_runner_mine(
                                        "[singletomulti fast]\n" + 
                                        "multiband.input.images=" + reflectance_mb + "\n"
                                        "multiband.output.image=" + reflectance_output_multiband_60M_ALL + "\n"
                                        )



    #=========================================================================
    thread_context.enter_stage("Cloud detection  - 60M")
    #=========================================================================

    # ALL / 60M

    # Do cloud detection on the tile (all bands required)
    # Use the blue, NIR and SWIR bands for cloud detection (60M resolution)

    # options :
    
    lowband_id = bandList.index(str(thread_context["low_band"]))
    cloud_low_id_string ="cloud.low.id="+  str(lowband_id) + "\n"
    cloud_low_threshold_string = "cloud.low.trh="+ str(thread_context["low_threshold"]) +"\n"
    average_threshold_string ="cloud.avg.trh="+  str(thread_context["average_threshold"]) + "\n"

    cirrus_threshold  = ""
    cirrus_band = ""
    if thread_context["cirrus"] == "true":
        cirrus_threshold = "cloud.cirrus.threshold=" + thread_context["cirrus_threshold"] + "\n"
        cirrus_band = "cloud.cirrus.band=10\n"

    cloud_mask_location_60m =  thread_context["prefix"] + "_CLOUDMASK_60M.tif"

    thread_context.invoke_ac_runner_mine(
        "[cloud detection]\n" +
        "cloud.input.location=" + reflectance_output_multiband_60M_ALL + "\n" +
         cloud_low_id_string +
        "cloud.high.id=8\n" +
         average_threshold_string +
         cloud_low_threshold_string +
        "cloud.mask.location=" + cloud_mask_location_60m + "\n" +
        "cloud.visible.bands=0 1 2 3 4 5 6 7 8\n"+
        cirrus_threshold +
        cirrus_band
        )

    #==============================================================================
    thread_context.enter_stage("Water detection")
    #==============================================================================

    water_mask_location_60m = thread_context["prefix"] + "_WATERMASK_60M.tif"
    water_mask_location_20m = thread_context["prefix"] + "_WATERMASK_20M.tif"
    water_mask_location_10m = thread_context["prefix"] + "_WATERMASK_10M.tif"
    
    band_name = str(thread_context["water_band"])
	
    if band_name in bandList_20m:
		water_mask_location = water_mask_location_20m
		water_input_location = reflectance_20m[band_name]
		water_orig_resolution=20

    elif band_name in bandList_10m:
        water_mask_location = water_mask_location_10m
        water_input_location = reflectance_10m[band_name]
        water_orig_resolution=10
    else:
        raise Exception("water detection band resolution should have either 10m or 20m resolution")
	
    water_band_string = "water.nir.band="+ str(0) +"\n"
    water_threshold = "water.treshold=" + thread_context["water_threshold"] + "\n"

    thread_context.invoke_ac_runner_mine(
        "[water detection]\n"
        "water.input.location=" + water_input_location + "\n"+ 
        water_band_string +
        water_threshold +
        "water.mask.location=" + water_mask_location  + "\n"
        )



    thread_context.invoke_ac_runner_mine(
                            "[resize mask]\n" + 
                            "resize.image.location=" + water_mask_location + "\n"+
                            "resize.reference.location="+ reflectance_60m['B01'] +"\n" +
                            "resize.destination.location=" + water_mask_location_60m +  "\n"
                            )

    if water_orig_resolution == 20:
            thread_context.invoke_ac_runner_mine(
                            "[resize mask]\n" + 
                            "resize.image.location=" + water_mask_location + "\n"+
                            "resize.reference.location="+ reflectance_10m['B02'] +"\n" +
                            "resize.destination.location=" + water_mask_location_10m +  "\n"
                            )

    
    if water_orig_resolution == 10:
            thread_context.invoke_ac_runner_mine(
                            "[resize mask]\n" + 
                            "resize.image.location=" + water_mask_location + "\n"+
                            "resize.reference.location="+ reflectance_20m['B05'] +"\n" +
                            "resize.destination.location=" + water_mask_location_20m +  "\n"
                            )



    if thread_context["watervapor"] == "true":

        #===================================================
        thread_context.enter_stage("Estimate Watervapor")
        #===================================================
        water_vapor_60M = thread_context["prefix"] + "_WATERVAPOR_60M.tif"
    
        # 60M
    

        thread_context.invoke_ac_runner_mine(
            "[watervapor]\n"
            "rl.tile.size=10\n"
            "rl.lut.location={ac_watcor_lut_all}\n"
            "rl.response.curves.location={ac_response_curves_all}\n"
            "rl.input.location=" + radiance_output_multiband_60M_ALL + "\n"
            "rl.output.location=" + water_vapor_60M + "\n"
            "rl.measurement.band=9\n"
            "rl.reference.band=8\n"
            "rl.override.sza=" + str(granule.mean_solar_zenith) + "\n" 
            "rl.override.vza=" + str(granule.mean_view_zenith) + "\n" 
            "rl.override.raa=" + str(granule.mean_relative_azimuth) + "\n"
            "rl.elevation.location=" + dem_list['60'] + "\n"
            "rl.override.aot=0.1\n"
            "rl.override.ozone=0.33\n"
            "rl.watermask.location=" + water_mask_location_60m + "\n"
            "rl.cloudmask.location=" + cloud_mask_location_60m + "\n"
            "rl.max.pixel.iterations=100"
            )

        
            # check if aot succeeded
        returnCodeWV = thread_context.invoke_ac_runner_check(
                        "[valid]\n"
                        "valid.input=" + water_vapor_60M + "\n"
                        )

        if returnCodeWV == 0:
            #=========================================================================
            thread_context.enter_stage("Resize Watervapor to 20M")
            #=========================================================================

            water_vapor_20M = thread_context["prefix"] + "_WATERVAPOR_20M.tif"
    

            thread_context.invoke_ac_runner_mine(
                                            "[resize fullpath]\n" + 
                                            "resize.image.location=" + water_vapor_60M + "\n"+
                                            "resize.reference.location="+ reflectance_20m['B05'] +"\n" +
                                            "resize.destination.location=" + water_vapor_20M +  "\n"
                                         )


            #=========================================================================
            thread_context.enter_stage("Resize Watervapor to 10M")
            #=========================================================================

            water_vapor_10M = thread_context["prefix"] + "_AOT_10M.tif"
    

            thread_context.invoke_ac_runner_mine(
                                            "[resize fullpath]\n" + 
                                            "resize.image.location=" + water_vapor_60M + "\n"+
                                            "resize.reference.location="+ reflectance_10m['B02'] +"\n" +
                                            "resize.destination.location=" + water_vapor_10M +  "\n"
                                         )


    if thread_context["aot"] == "true":
        
        aot_window_string = "aot.square.pixels=" + str(thread_context["aot_window_size"]) + "\n" 
        aot_ozone_override_string = "aot.override.ozone="+ thread_context["ozone_override"] +"\n"         

        if thread_context["watervapor"] == "true":
           aot_wv_string = "aot.input.watervapor.location=" + water_vapor_60M + "\n"

        else:
            aot_wv_string = "aot.override.watervapor=" + thread_context["watervapor_override"] + "\n"


        #=========================================================================
        thread_context.enter_stage("Estimate the Aerosol Optical Thickness (AOT)")
        #=========================================================================
        aot_location_60M = thread_context["prefix"] + "_AOT_60M.tif"

        thread_context.invoke_ac_runner_mine(
            "[aot guanter]\n" 
            "aot.lut.location={ac_watcor_lut_all}\n" 
            "aot.response.curves.location={ac_response_curves_all}\n" 
            "aot.solarirradiance.location={ac_solar_irradiance}\n" 
            "aot.input.location=" + radiance_output_multiband_60M_ALL + "\n" 
            "aot.output.location=" + aot_location_60M + "\n" 
            "aot.image.bands=0 1 2 3 4 5 6 7 8\n" 
            "aot.image.visible.bands=0 1 2 3 4\n"  +
             aot_window_string+
            "aot.ndvi.bands=3 6\n" 
            "aot.ndvi.list=0.01 0.10 0.45 0.7\n" 
            "aot.ndvi.refined.bands=3 8\n" 
            "aot.refpixels.nr=5\n" 
            "aot.limit.refsets=5\n"
            "aot.weights=2.0 2.0 1.5 1.5 1.0\n" 
            "aot.centerwl.inverse.location={ac_inverse_profiles}\n" 
            "aot.vegetation.profiles={ac_vegetation_profiles}\n" 
            "aot.sand.profiles={ac_soil_profiles}\n" 
            "aot.watermask.location=" + water_mask_location_60m + "\n" 
            "aot.cloudmask.location=" + cloud_mask_location_60m + "\n" 
            "aot.cloudmask.dilate=10\n"
            "aot.override.sza=" + str(granule.mean_solar_zenith) + "\n" 
            "aot.override.vza=" + str(granule.mean_view_zenith) + "\n" 
            "aot.override.raa=" + str(granule.mean_relative_azimuth) + "\n" +
            aot_ozone_override_string +
            aot_wv_string +
            "aot.input.elevation.location=" + dem_list['60'] + "\n"
            )
        
        
        # check if aot succeeded
        returnCodeAOT = thread_context.invoke_ac_runner_check(
                        "[valid]\n"
                        "valid.input=" + aot_location_60M + "\n"
                        )
        if returnCodeAOT == 0:
            thread_context.add_keep_tmp(aot_location_60M)
            #=========================================================================
            thread_context.enter_stage("Resize AOT to 20M")
            #=========================================================================

            aot_location_20M = thread_context["prefix"] + "_AOT_20M.tif"
    

            thread_context.invoke_ac_runner_mine(
                                            "[resize fullpath]\n" + 
                                            "resize.image.location=" + aot_location_60M + "\n"+
                                            "resize.reference.location="+ reflectance_20m['B05'] +"\n" +
                                            "resize.destination.location=" + aot_location_20M +  "\n"
                                         )


            #=========================================================================
            thread_context.enter_stage("Resize AOT to 10M")
            #=========================================================================

            aot_location_10M = thread_context["prefix"] + "_AOT_10M.tif"
    

            thread_context.invoke_ac_runner_mine(
                                            "[resize fullpath]\n" + 
                                            "resize.image.location=" + aot_location_60M + "\n"+
                                            "resize.reference.location="+ reflectance_10m['B02'] +"\n" +
                                            "resize.destination.location=" + aot_location_10M +  "\n"
                                         )




    if thread_context["simec"] == "true":

        #=========================================================================
        thread_context.enter_stage("Estimate the Background Radiance (SIMEC)")
        #=========================================================================
        

        simec_location_60m = thread_context["prefix"] + "_SIMEC_60M.tif"
    
        simec_window_string = "simec.square.pixels=" + str(thread_context["bg_window"]) + "\n" 
        simec_ozone_override_string = "simec.override.ozone="+ thread_context["ozone_override"] +"\n"         

        if thread_context["watervapor"] == "true" and returnCodeWV == 0:
           simec_wv_string = "simec.watervapor.location=" + water_vapor_60M + "\n"

        else:
            simec_wv_string = "simec.override.watervapor=" + thread_context["watervapor_override"] + "\n"

        if thread_context["aot"] == "true" and returnCodeAOT == 0 :
            simec_aot_string = "simec.aot.location="+ aot_location_60M + "\n"
        else:
            simec_aot_string = "simec.override.aot=" + thread_context["aot_override"] + "\n"


        thread_context.invoke_ac_runner_mine(
            "[simec]\n" +
            "simec.lut.location={ac_watcor_lut_all}\n" +
            "simec.response.curves.location={ac_response_curves_all}\n" +
            "simec.radiance.location=" + radiance_output_multiband_60M_ALL + "\n" +
            "simec.subsample.factor=5\n" + 
            "simec.subsample.band=8\n" +
            "simec.nir.band=4\n" +
            "simec.nir780.band=6\n" +
            "simec.lut.band.nir=4\n" +
            "simec.lut.band.nir780=6\n" + 
            "simec.max.window=100\n" +
            "simec.sensor.resolution_km=0.3\n" +
            "simec.override.sza=" + str(granule.mean_solar_zenith) + "\n" +
            "simec.override.vza=" + str(granule.mean_view_zenith) + "\n" +
            "simec.override.raa=" + str(granule.mean_relative_azimuth) + "\n" +
            "simec.watermask.location=" + water_mask_location_60m + "\n" +
            "simec.cloudmask.location="+ cloud_mask_location_60m + "\n" +
            "simec.nir.similarity.location={ac_near_similarity_refl}\n" +
            "simec.elevation.location=" + dem_list['60'] + "\n" +
             simec_aot_string +
             simec_wv_string +
             simec_ozone_override_string +
            "simec.output.location=" + simec_location_60m + "\n" +
            "simec.default.background.size=" + str(thread_context["bg_window"]) + "\n"
            )

        thread_context.add_keep_tmp(simec_location_60m)

        # Split the bands and resize the output of simec
    

        #=========================================================================
        thread_context.enter_stage("Extract SIMEC bands 20M")
        #=========================================================================


        simex_extract_location_20M = thread_context["prefix"] + "_SIMEC_EXTRACT_20M.tif"

        extract_band_list=""
        for band in bandList_20m:
            extract_band_list += str(bandList.index(band)) + " "

        thread_context.invoke_ac_runner_mine(
                                        "[extract]\n" + 
                                        "extract.input.location=" + simec_location_60m + "\n"+
                                        "extract.output.location="+ simex_extract_location_20M +"\n" +
                                        "extract.bands=" + extract_band_list + "\n"
                                        )


        #=========================================================================
        thread_context.enter_stage("Extract SIMEC bands 10M")
        #=========================================================================


        simex_extract_location_10M = thread_context["prefix"] + "_SIMEC_EXTRACT_10M.tif"

        extract_band_list=""
        for band in bandList_10m:
            extract_band_list += str(bandList.index(band)) + " "

        thread_context.invoke_ac_runner_mine(
                                        "[extract]\n" + 
                                        "extract.input.location=" + simec_location_60m + "\n"+
                                        "extract.output.location="+ simex_extract_location_10M +"\n" +
                                        "extract.bands=" + extract_band_list + "\n"
                                     )

    
        #=========================================================================
        thread_context.enter_stage("Resize BACKGROUND to 20M")
        #=========================================================================

        simec_location_20m = thread_context["prefix"] + "_SIMEC_20M.tif"
    

        thread_context.invoke_ac_runner_mine(
                                        "[resize fullpath]\n" + 
                                        "resize.image.location=" + simex_extract_location_20M + "\n"+
                                        "resize.reference.location="+ reflectance_20m['B05'] +"\n" +
                                        "resize.destination.location=" + simec_location_20m +  "\n"
                                     )


        #=========================================================================
        thread_context.enter_stage("Resize BACKGROUND to 10M")
        #=========================================================================

        simec_location_10m = thread_context["prefix"] + "_SIMEC_10M.tif"
    

        thread_context.invoke_ac_runner_mine(
                                        "[resize fullpath]\n" + 
                                        "resize.image.location=" + simex_extract_location_10M + "\n"+
                                        "resize.reference.location="+ reflectance_10m['B02'] +"\n" +
                                        "resize.destination.location=" + simec_location_10m +  "\n"
                                     )




    atm_aot_string = "atm.override.aot=" + thread_context["aot_override"] + "\n" 
    atm_background_string = "atm.override.background=" + str(thread_context["bg_window"]) + "\n"
    atm_ozone_override_string = "atm.override.ozone="+ thread_context["ozone_override"] +"\n"
    atm_watervapor_string = "atm.override.watervapor=" + thread_context["watervapor_override"] + "\n"


    # construnct an output name 
    out_filename = thread_context["output_file"]

    out_list = os.path.splitext(out_filename)

    

    output_basename = out_list[0] + "_" + thread_context["name"]
    extension = out_list[1]


    #=========================================================================
    thread_context.enter_stage("Atmospheric Correction - 10M")
    #=========================================================================

    result_10m =  output_basename + "_10M" + extension

    if thread_context["watervapor"] == "true" and returnCodeWV == 0:
        atm_watervapor_string = "atm.watervapor.location="+ water_vapor_10M + "\n"


    if thread_context["aot"] == "true" and returnCodeAOT == 0 :
        atm_aot_string = "atm.aot.location="+aot_location_10M + "\n"

    if thread_context["simec"] == "true":
       atm_background_string = "atm.background.location=" + simec_location_10m + "\n"

    # 10M
    
    thread_context.invoke_ac_runner_mine(
        "[watcor]\n"
        "atm.lut.location={ac_watcor_lut_10m}\n" +
        "atm.radiance.location=" + radiance_output_multiband_10M + "\n" +
        "atm.override.sza=" + str(granule.mean_solar_zenith) + "\n" +
        "atm.override.vza=" + str(granule.mean_view_zenith) + "\n" +
        "atm.override.raa=" + str(granule.mean_relative_azimuth) + "\n" +
        "atm.elevation.location=" + dem_list['10'] + "\n" +
        "atm.watermask.location=" + water_mask_location_10m + "\n" +
        atm_background_string +
        atm_aot_string +
        atm_ozone_override_string +
        atm_watervapor_string +
        "atm.output.location=" + result_10m
        )


    #20M
  

    #=========================================================================
    thread_context.enter_stage("Atmospheric Correction - 20M")
    #=========================================================================
    
    result_20m = result_10m =  output_basename + "_20M" + extension
    # 20M
    if thread_context["watervapor"] == "true" and returnCodeWV == 0:
        atm_watervapor_string = "atm.watervapor.location="+ water_vapor_20M + "\n"


    if thread_context["aot"] == "true" and returnCodeAOT == 0 :
        atm_aot_string = "atm.aot.location="+aot_location_20M + "\n"

    if thread_context["simec"] == "true":
       atm_background_string = "atm.background.location=" + simec_location_20m + "\n"



    thread_context.invoke_ac_runner_mine(
        "[watcor]\n"
        "atm.lut.location={ac_watcor_lut_20m}\n" +
        "atm.radiance.location=" + radiance_output_multiband_20M + "\n" +
        "atm.override.sza=" + str(granule.mean_solar_zenith) + "\n" +
        "atm.override.vza=" + str(granule.mean_view_zenith) + "\n" +
        "atm.override.raa=" + str(granule.mean_relative_azimuth) + "\n" +
        "atm.elevation.location=" + dem_list['20'] + "\n" +
        "atm.watermask.location=" + water_mask_location_20m + "\n" +
        atm_background_string +
        atm_aot_string +
        atm_ozone_override_string +
        atm_watervapor_string +
        "atm.output.location=" + result_20m + "\n"
        )



    #60M

    #=========================================================================
    thread_context.enter_stage("Atmospheric Correction - 60M")
    #=========================================================================
    result_60m = output_basename +  "_60M" + extension

    #result_60m = thread_context["output_file"]
    
    if thread_context["watervapor"] == "true" and returnCodeWV == 0:
        atm_watervapor_string = "atm.watervapor.location="+ water_vapor_60M + "\n"

    if thread_context["aot"] == "true" and returnCodeAOT == 0 :
        atm_aot_string = "atm.aot.location=" + aot_location_60M + "\n"

    if thread_context["simec"] == "true":
       atm_background_string = "atm.background.location=" + simec_location_60m + "\n"


    # 60M
    
    thread_context.invoke_ac_runner_mine(
        "[watcor]\n"
        "atm.lut.location={ac_watcor_lut_all}\n" +
        "atm.radiance.location=" + radiance_output_multiband_60M_ALL + "\n" +
        "atm.override.sza=" + str(granule.mean_solar_zenith) + "\n" +
        "atm.override.vza=" + str(granule.mean_view_zenith) + "\n" +
        "atm.override.raa=" + str(granule.mean_relative_azimuth) + "\n" +
        "atm.elevation.location=" + dem_list['60'] + "\n" +
        "atm.watermask.location=" + water_mask_location_60m + "\n" +
        atm_background_string +
        atm_aot_string +
        atm_ozone_override_string +
        atm_watervapor_string +
        "atm.output.location=" + result_60m  + "\n" +
        "atm.radiance.bands=0 1 2 3 4 5 6 7 8 11 12\n"
        )


    #add intermediates    
    thread_context.add_keep_tmp(water_mask_location_60m)
    thread_context.add_keep_tmp(cloud_mask_location_60m)
    

    #=========================================================================
    thread_context.enter_stage("Remove intermediate files from filesystem")
    #========================================================================= 

    keep_tmp = False    

    if thread_context["keep_intermediate"] == "true":
        keep_tmp = True
    
    thread_context.remove_tmp_files(granule_working_dir,keep_tmp,granule.get_output_folder())

    print "iCOR Atmospheric correction done for product : " + thread_context["prefix"]
