#!/usr/bin/env python

import os
import os.path
import glob
import threading

import context

import sentinel3_metadata

 


def process(context, path, dir=os.getcwd()):



    # ------------------------------------------------------------------------
    # WORKFLOW Sentinel 3: iCOR
    # ------------------------------------------------------------------------




    context["aot_window_size"] = 50
    


    apply_svc_gains = False
    if(context["apply_svc"]=="true"):
        apply_svc_gains = True

    use_watervapor_product = False
    if context["watervapor"] == "true":
        use_watervapor_product = True

    use_ozone_product = False
    if context["ozone"] == "true":
        use_ozone_product = True

    max_stages = 10

    if context["productwatermask"] == "false": +1


    if context["aot"] == "true": 
        max_stages += 5

    if apply_svc_gains :
        max_stages += 2

    if context["simec"] == "true" : 
        max_stages += 1

    if( context["keep_land"] == "true" ): 
        max_stages += 2

    elif ( context["keep_water"] == "true" ):
        max_stages += 2

    if context["projectoutput"] == "true": 
        max_stages += 1

    context["max_stages"] = max_stages
    
    metadata_file = str(path)
    
    metadata = sentinel3_metadata.parse(metadata_file);
    
    distance_scale = metadata.get_distance_correction()

    productname = metadata.get_productname()

    print "processing product with name : " + productname

    output_file = context["output_file"]
    output_folder = os.path.dirname(output_file)

    sensorname= str(context["sensorname"])

    input_location = os.path.dirname(path);


    #working folder and name prefix
    location = dir + "/" +  productname + "_"
    

    context.write_config(location)

    

    radiance_name = "OaNN_radiance"

    #=========================================================================
    context.enter_stage("Convert netCDF ")
    #=========================================================================

    
    band_filenames = []
    
    #bands
    for band in sentinel3_metadata.bands:

        band_name = radiance_name.replace("NN",str(band))
        out_name = location + band_name + ".tif"
        band_filenames.append(out_name)
        context.invoke_netcdf_tools("[extract_s3_level1]\n"
                                "name=" + band_name  + "\n" +
                                "input.location="+ os.path.join(input_location ,band_name )+ ".nc" + "\n" +
                                "output.location=" + out_name +  "\n" +
                                "scale.value=" + str(distance_scale*distance_scale) + "\n"
                                )

    
    name = "geo_location"
    
    context.invoke_netcdf_tools("[extract_s3_level1]\n"
                                "name=" + name  + "\n" +
                                "input.location="+ os.path.join(input_location , "geo_coordinates.nc") + "\n" +
                                "output.location=" + location + "geo"+  "\n"
                                "scale.altitude=0.001\n"
                               )

    elevation_location = location+ "geo_altitude.tif"

    name = "angles"
    
    context.invoke_netcdf_tools("[extract_s3_level1]\n"
                                "name=" + name  + "\n" +
                                "input.location="+ os.path.join(input_location ,"tie_geometries.nc") + "\n" +
                                "output.location=" + location + "angles"+  "\n"
                                )
    
    
    name = "meteo"
    
    context.invoke_netcdf_tools("[extract_s3_level1]\n"
                                "name=" + name  + "\n" +
                                "input.location="+ os.path.join(input_location ,"tie_meteo.nc") + "\n" +
                                "output.location=" + location + "meteo"+  "\n"
                                "scale.ozone=46.698\n"  +
                                "scale.watervapor=0.1\n" 
                                 )

                              

    watervapor_input_location = location + "meteo_watervapor.tif"
    ozone_input_location = location + "meteo_ozone.tif"
    


    name="mask"
    productwatermask = location  + "productwatermask.tif"
    context.invoke_netcdf_tools("[extract_s3_level1]\n"
                        "name=" + name  + "\n" +
                        "input.location="+ os.path.join(input_location ,"qualityFlags.nc") + "\n" +
                        "output.location=" + productwatermask +  "\n"
                        "flag.value=2147483648\n"  +
                        "fill.value=33554432\n" +
                        "invert.mask=true"
                        )

       
    inlandwatermask = location  + "inlandwatermask.tif"
    context.invoke_netcdf_tools("[extract_s3_level1]\n"
                            "name=" + name  + "\n" +
                            "input.location="+ os.path.join(input_location , "qualityFlags.nc" ) + "\n" +
                            "output.location=" + inlandwatermask +  "\n" +
                            "flag.value=536870912\n"  +
                            "fill.value=33554432\n" +
                            "invert.mask=false"
                            )

    inversinlandwatermask = location  + "inverseinlandwatermask.tif"
    context.invoke_netcdf_tools("[extract_s3_level1]\n"
                            "name=" + name  + "\n" +
                            "input.location="+ os.path.join(input_location , "qualityFlags.nc" ) + "\n" +
                            "output.location=" + inversinlandwatermask +  "\n" +
                            "flag.value=536870912\n"  +
                            "fill.value=33554432\n" +
                            "invert.mask=true"
                            )


    landmask = location + "landmask.tif"
    context.invoke_netcdf_tools("[extract_s3_level1]\n"
                        "name=" + name  + "\n" +
                        "input.location="+ os.path.join(input_location ,"qualityFlags.nc") + "\n" +
                        "output.location=" + landmask +  "\n"
                        "flag.value=2147483648\n"  +
                        "fill.value=33554432\n" +
                        "invert.mask=false"
                        )

    
    #=========================================================================
    context.enter_stage("Relative Azimuth")
    #=========================================================================

    # raa
    context.invoke_ac_runner_check("[relative azimuth]\n" + 
                                            "relative.azimuth.saa.location=" + location + "angles_saa.tif" + "\n"+
                                            "relative.azimuth.vaa.location="+ location + "angles_vaa.tif" + "\n" +
                                            "relative.azimuth.raa.location=" + location + "angles_raa.tif"  +  "\n"
                                         )


    #=========================================================================
    context.enter_stage("Resize images")
    #=========================================================================


    #vza sza and raa
    
    
    relazimuth_location = location + "angles_raa_resize.tif"

    context.invoke_ac_runner_check("[resize fullpath]\n" + 
                                            "resize.image.location=" + location + "angles_raa.tif" + "\n"+
                                            "resize.reference.location="+ band_filenames[0] +"\n" +
                                            "resize.destination.location=" + relazimuth_location +  "\n"
                                         )

    solarzenith_location = location + "angles_sza_resize.tif"

    context.invoke_ac_runner_check("[resize fullpath]\n" + 
                                            "resize.image.location=" + location + "angles_sza.tif" + "\n"+
                                            "resize.reference.location="+ band_filenames[0] +"\n" +
                                            "resize.destination.location=" + solarzenith_location +  "\n"
                                         )

        
    viewzenith_location = location + "angles_vza_resize.tif"

    context.invoke_ac_runner_check("[resize fullpath]\n" + 
                                            "resize.image.location=" + location + "angles_vza.tif" + "\n"+
                                            "resize.reference.location="+ band_filenames[0] +"\n" +
                                            "resize.destination.location=" + viewzenith_location +  "\n"
                                         )

       

    watervapor_location = location + "watervapor.tif"

    context.invoke_ac_runner_check("[resize fullpath]\n" + 
                                            "resize.image.location=" +   watervapor_input_location + "\n"+
                                            "resize.reference.location="+ band_filenames[0] +"\n" +
                                            "resize.destination.location=" + watervapor_location +  "\n"
                                         )

    ozone_location = location + "ozone.tif"

    context.invoke_ac_runner_check("[resize fullpath]\n" + 
                                  "resize.image.location="          + ozone_input_location + "\n"+
                                  "resize.reference.location="      + band_filenames[0] +"\n" +
                                  "resize.destination.location="    + ozone_location +  "\n"
                                  )

    

    #=========================================================================
    context.enter_stage("Multiband - Radiance")
    #=========================================================================


    #single to multiband

    radiance_input = ""
    radiance_output = location + "scaled_radiance.tif"

    for name in band_filenames:
        radiance_input += str(name)
        radiance_input += " "


    context.invoke_ac_runner_check(        
                                    "[singletomulti fast]\n" + 
                                    "multiband.input.images=" + radiance_input + "\n" +
                                    "multiband.output.image=" + radiance_output + "\n"
                                    )
    




    #=========================================================================
    context.enter_stage("Reflectance")
    #=========================================================================


    #reflectance

    reflectance_output = location + "reflectance.tif"
    context.invoke_ac_runner_check(
                                        "[reflectance]\n" +
                                        "reflectance.input.radiance.location=" + radiance_output + "\n"+
                                        "reflectance.image.dayofyear=94\n"+
                                        "reflectance.bands=0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20\n"+
                                        "reflectance.lut.bands=0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20\n"
                                        "reflectance.destination.location="+ reflectance_output + "\n"+
                                        "reflectance.input.sza.location=" + location + "angles_sza_resize.tif" + "\n"+
                                        "reflectance.solarirradiance.location={ac_solar_irradiance}\n"+
                                        "reflectance.response.curves.location={ac_response_curves}\n"
                                        )


    #=========================================================================
    context.enter_stage("Cloud Mask")
    #=========================================================================


    cloudmask = location + "cloudmask.tif"

    low_id = sentinel3_metadata.get_band_id(str(context["low_band"]))

    low_threshold = context["low_threshold"]
    average_threshold = context["average_threshold"]



    context.invoke_ac_runner_check(
        "[cloud detection]\n" +
        "cloud.input.location=" + reflectance_output + "\n" +
        "cloud.low.id=" + str(low_id) + "\n" + #B02 default !! 
        "cloud.high.id=9\n" +
        "cloud.avg.trh= " + str(average_threshold) + "\n" +
        "cloud.low.trh= " + str(low_threshold)  + "\n" +
        "cloud.mask.location=" + cloudmask + "\n" +
        "cloud.visible.bands=0 1 2 3 4 5 6 7 8 9\n"
        )
    
    # define which watermask are to be used in the processing

    watermask = location + "watermask.tif"
    appliedwatermask = watermask

    if context["productwatermask"] == "false":

        #=========================================================================
        context.enter_stage("Water Detection")
        #=========================================================================

        band_name = str(context["water_band"])
        threshold = str(context["water_threshold"])
        
        context.invoke_ac_runner_check(
            "[water detection]\n"
            "water.input.location=" + reflectance_output + "\n" 
            "water.nir.band=" + str(sentinel3_metadata.get_band_id(band_name)) + "\n" +
            "water.treshold="+ threshold +"\n" +
            "water.mask.location="+ watermask +"\n"
            )

        simecwatermask = watermask
        
        
        
    elif context["productwatermask"] == "true":

        #=========================================================================
        context.enter_stage("Merge Masks - product and inland watermask")
        #=========================================================================
        context.invoke_ac_runner_check(
                                "[merge masks]\n"
                                "merge.masks=" + productwatermask + " " + inlandwatermask + "\n" +
                                "merge.output.location=" + watermask + "\n"
                                "merge.sum=true\n" 
                                )

        simecwatermask = watermask
        
        if context["inlandwater"] == "true":
            simecwatermask = inlandwatermask
            appliedwatermask = inlandwatermask
            
    
    aotwatermask = watermask
    #also set the land/water mask that will be applied to the final masking of the product




    if context["aot"] == "true": 
        
        #=========================================================================
        context.enter_stage("Calculate Aerosol Optical Thickness (AOT) LAND")
        #=========================================================================
        aot_output =""
    
        aot_guanter_output = location + "aot_land.tif"


        aot_window_string = "aot.square.pixels=" + str(context["aot_window_size"]) + "\n" 
        
        wvstring = "aot.override.watervapor=" + context["watervapor_override"] + "\n"
        if use_watervapor_product:
            wvstring = "aot.input.watervapor.location="+ watervapor_location + "\n"

        ozonestring = "aot.override.ozone=" + context["ozone_override"] + "\n"
        if use_ozone_product:
            ozonestring = "aot.input.ozone.location=" + ozone_location  + "\n"


        context.invoke_ac_runner_check(
                        "[aot guanter]\n" 
                        "aot.lut.location={ac_watcor_lut}\n" 
                        "aot.response.curves.location={ac_response_curves}\n" 
                        "aot.solarirradiance.location={ac_solar_irradiance}\n" 
                        "aot.input.location="+ radiance_output +"\n" 
                        "aot.output.location=" + aot_guanter_output + "\n" 
                        "aot.image.visible.bands=0 1 2 3 4 5 6 7 8 9\n" 
                        "aot.image.bands=0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18\n"  +
                         aot_window_string +
                        "aot.ndvi.bands=7 15\n" 
                        "aot.ndvi.list=0.01 0.10 0.45 0.7\n" 
                        "aot.ndvi.refined.bands=9 16\n" 
                        "aot.refpixels.nr=5\n"
                        "aot.limit.refsets=5\n"
                        "aot.weights=2.0 2.0 1.5 1.5 1.0\n" 
                        "aot.centerwl.inverse.location={ac_inverse_profiles}\n" 
                        "aot.vegetation.profiles={ac_vegetation_profiles}\n" 
                        "aot.sand.profiles={ac_soil_profiles}\n" 
                        "aot.watermask.location=" + aotwatermask + "\n" 
                        "aot.cloudmask.location=" + cloudmask + "\n" 
                        "aot.cloudmask.dilate=10\n"
                        "aot.input.sza.location=" + solarzenith_location +  "\n" 
                        "aot.input.vza.location=" + viewzenith_location + "\n" 
                        "aot.input.raa.location=" + relazimuth_location + "\n" +
                         wvstring +
                         ozonestring +
                        "aot.input.elevation.location="+ elevation_location + "\n"
                        "aot.criterion=true"
                        )

                    # check if aot succeeded
        context.invoke_ac_runner_check(
                        "[valid]\n"
                        "valid.input=" + aot_guanter_output + "\n"
                        )

        #================================================================================
        context.enter_stage("Calculate Aerosol Optical Thickness (AOT) WATER - LOW BAND")
        #================================================================================
        wvstring = "aotswir.override.cwv=" + context["watervapor_override"] + "\n"
        if use_watervapor_product:
            wvstring = "aotswir.cwv.location="+ watervapor_location + "\n"

        ozonestring = "aotswir.override.ozone=" + context["ozone_override"] + "\n"
        if use_watervapor_product:
            ozonestring = "aotswir.ozone.location=" + ozone_location  + "\n"


        water_aot_first_output = location + "aot_water_first.tif"
        model_location = location + "aot_model.tif"
        threshold_location = location + "aot_threshold.tif"
        glint_mask = location + "glintmask.tif"

        context.invoke_ac_runner_check (
                
                        "[aotswir]\n"
                        "aotswir.lut.list.location={ac_watcor_lut}\n" +
                        "aotswir.solarirradiance.location={ac_solar_irradiance}\n" 
                        "aotswir.response.curves.location={ac_response_curves}\n" 
                        "aotswir.band.low=10\n"
                        "aotswir.band.high=10\n"
                        "aotswir.radiance.location=" + radiance_output + "\n"
                        "aotswir.watermask.location=" + watermask + "\n" 
                        "aotswir.output.location=" + water_aot_first_output + "\n"
                        "aotswir.output.model.location=" + model_location + "\n"
                        "aotswir.output.threshold.location=" + threshold_location + "\n"
                        "aotswir.cloudmask.location=" + cloudmask + "\n"
                        "aotswir.elevation.location=" + elevation_location + "\n"
                        "aotswir.sza.location=" + solarzenith_location + "\n"
                        "aotswir.vza.location=" + viewzenith_location + "\n"
                        "aotswir.raa.location=" + relazimuth_location + "\n"+

                        ozonestring +
                        wvstring +
                        
                        "aotswir.clearpixel.threshold=0.8\n"
                        "aotswir.aot.threshold=1.2\n"
                        "aotswir.median.filter.size=3\n"

                        "aotswir.windspeed=5.0\n"
                        "aotswir.glint.reflectance.threshold=0.5\n"
                        "aotswir.glint.calculate=true\n"
                        "aotswir.glint.mask.location=" + glint_mask + "\n"

                        )

        #====================================================================================
        context.enter_stage("Calculate Aerosol Optical Thickness (AOT) WATER - SECOND BAND")
        #====================================================================================

        water_aot_second_output = location + "aot_water_second.tif"
        model_location = location + "aot_model.tif"
        threshold_location = location + "aot_threshold.tif"
        

        context.invoke_ac_runner_check (
                
                        "[aotswir]\n"
                        "aotswir.lut.list.location={ac_watcor_lut}\n" +
                        "aotswir.solarirradiance.location={ac_solar_irradiance}\n" 
                        "aotswir.response.curves.location={ac_response_curves}\n" 
                        "aotswir.band.low=17\n"
                        "aotswir.band.high=17\n"
                        "aotswir.radiance.location=" + radiance_output + "\n"
                        "aotswir.watermask.location=" + watermask + "\n" 
                        "aotswir.output.location=" + water_aot_second_output + "\n"
                        "aotswir.output.model.location=" + model_location + "\n"
                        "aotswir.output.threshold.location=" + threshold_location + "\n"
                        "aotswir.cloudmask.location=" + cloudmask + "\n"
                        "aotswir.elevation.location=" + elevation_location + "\n"
                        "aotswir.sza.location=" + solarzenith_location + "\n"
                        "aotswir.vza.location=" + viewzenith_location + "\n"
                        "aotswir.raa.location=" + relazimuth_location + "\n"+

                        
                        ozonestring +
                        wvstring+
                    
                        "aotswir.clearpixel.threshold=0.8\n"
                        "aotswir.aot.threshold=1.2\n"
                        "aotswir.median.filter.size=3\n"

                        "aotswir.windspeed=5.0\n"
                        "aotswir.glint.reflectance.threshold=0.5\n"
                        "aotswir.glint.calculate=true\n"
                        "aotswir.glint.mask.location=" + glint_mask + "\n"

                        )

        #====================================================================================
        context.enter_stage("Select lowest AOT")
        #====================================================================================

        water_aot_output = location + "aot_water.tif"

        context.invoke_ac_runner_check (
                
                        "[select minimum]\n"
                        "select.1.location=" + water_aot_first_output + "\n"
                        "select.2.location=" + water_aot_second_output + "\n"
                        "select.output.location=" + water_aot_output  + "\n"

                        )

    
        #=========================================================================
        context.enter_stage("Merge Both AOT rasters")
        #=========================================================================

        aot_output = location + "aot.tif"

        context.invoke_ac_runner_check (
                
                        "[merge]\n"
                        "merge.mask.location="+ watermask + "\n" +
                        "merge.reference.location="+ aot_guanter_output + "\n" 
                        "merge.subgrid.location="+ water_aot_output + "\n" 
                        "merge.output.location="+ aot_output +"\n" 
                    
                        )



    if apply_svc_gains:

        #=========================================================================
        context.enter_stage("Apply SVC gains")
        #=========================================================================

        svc_list = sentinel3_metadata.get_svc(sensorname)

        scaled_image = location + "scaled"

        scaled_band_filenames = []

        for band in sentinel3_metadata.bands:

            svc_id = sentinel3_metadata.bands.index(band)

            context.invoke_ac_runner_check (
                
                            "[scale]\n"
                            "scale.gain="+ str(svc_list[svc_id]) + "\n" +
                            "scale.offset=0.0\n" 
                            "scale.input.location=" + band_filenames[svc_id] + "\n" +
                            "scale.output.location=" + scaled_image + str(band) + "_svc_applied.tif\n"
                            "scale.invalid.minimum=0.0\n"
                            "scale.zero.invalid=0.0\n"
                            )


            scaled_band_filenames.append(scaled_image + str(band) + "_svc_applied.tif")



        #=========================================================================
        context.enter_stage("Multiband - Radiance SVC")
        #=========================================================================


        #single to multiband

        radiance_input = ""
        radiance_output = location + "scaled_radiance_svc.tif"

        for name in scaled_band_filenames:
            radiance_input += str(name)
            radiance_input += " "


        context.invoke_ac_runner_check(        
                                        "[singletomulti fast]\n" + 
                                        "multiband.input.images=" + radiance_input + "\n" +
                                        "multiband.output.image=" + radiance_output + "\n"
                                        )
    

    simec_output=""
    if context["simec"] == "true" :
        
        #=========================================================================
        context.enter_stage("Use SIMEC to calculate the background")
        #=========================================================================

        simec_output = location + "simec.tif" + "\n"

        aot_string = "simec.override.aot=" + str(context["aot_override"]) + "\n"
        if context["aot"] == "true":
            aot_string = "simec.aot.location= " + aot_output + "\n"


        context.invoke_ac_runner_check(
                       "[simec]\n" +
                        "simec.lut.location={ac_watcor_lut}\n" +
                        "simec.response.curves.location={ac_response_curves}\n" +
                        "simec.radiance.location=" + radiance_output + "\n"
                        "simec.subsample.factor=1\n" + 
                        "simec.subsample.band=5\n" +
                        "simec.nir.band=10\n" +
                        "simec.nir780.band=15\n" +
                        "simec.lut.band.nir=10\n" +
                        "simec.lut.band.nir780=15\n" + 
                        "simec.max.window=100\n" +
                        "simec.sensor.resolution_km=0.33\n" +
                        "simec.sza.location=" + solarzenith_location + "\n" +
                        "simec.vza.location=" + viewzenith_location + "\n" +
                        "simec.raa.location=" + relazimuth_location + "\n" +
                        "simec.watermask.location="+ simecwatermask + "\n" +
                        "simec.cloudmask.location="+ cloudmask + "\n" +
                        "simec.nir.similarity.location={ac_near_similarity_refl}\n" +
                        "simec.elevation.location=" + elevation_location + "\n" +
                         aot_string +
                        "simec.watervapor.location=" + watervapor_location +  "\n"
                        "simec.ozone.location=" + ozone_location + "\n"
                        "simec.default.background.size=1\n" +
                        "simec.output.location=" + simec_output +"\n"
                        )

    


    # pixel masking with the correct mask
    masked_uncorrected_output = location + "masked_radiance.tif"

    if( context["keep_land"] == "true" ):

        #=========================================================================
        context.enter_stage("Masking Pixels")
        #=========================================================================
        context.invoke_ac_runner_check(

                            "[mask removal]\n"
                            "input.location=" + radiance_output + "\n" +
                            "output.location=" + masked_uncorrected_output + "\n" +
                            "mask.list=" + appliedwatermask + " " + cloudmask + "\n" 

                            )
        

    elif context["keep_water"] == "true" :

        invertmask = location + "invertedwatermask.tif"

        #=========================================================================
        context.enter_stage("Invert Watermask")
        #=========================================================================

        context.invoke_ac_runner_check(

                            "[invert mask]\n"
                            "input.location=" + appliedwatermask + "\n" +
                            "output.location=" + invertmask + "\n"
                            )
        
        #=========================================================================
        context.enter_stage("Masking Pixels")
        #=========================================================================
        context.invoke_ac_runner_check(

                            "[mask removal]\n"
                            "input.location=" + radiance_output + "\n" +
                            "output.location=" + masked_uncorrected_output + "\n" +
                            "mask.list=" + invertmask +  " " + cloudmask + "\n" 
                        
                            )
           
    else:
        masked_uncorrected_output = radiance_output




    #=========================================================================
    context.enter_stage("Atmospheric correction")
    #=========================================================================
    aot_string = "atm.override.aot=" + str(context["aot_override"]) + "\n"
    if context["aot"] == "true":        
        aot_string = "atm.aot.location=" + aot_output + "\n" 
        context.add_keep_tmp(aot_output)



    surface_reflectance_output = location + "surface_reflectance_watcor.tif"
    
    simec_string = "atm.override.background=" + str(context["bg_window"]) + "\n"

    if context["simec"] == "true" :
        simec_string = "atm.background.location=" + simec_output + "\n" 

    
    bands_final = ""
    for band in sentinel3_metadata.bands_final:
        bands_final += str(band) + " "

    bands_final +="\n"

    context.invoke_ac_runner_check(
                        "[watcor]\n"
                        "atm.lut.location={ac_watcor_lut}\n" +
                        "atm.radiance.location=" + masked_uncorrected_output + "\n" +
                        "atm.sza.location=" + solarzenith_location + "\n" +
                        "atm.vza.location=" + viewzenith_location + "\n" +
                        "atm.raa.location=" + relazimuth_location + "\n" +
                        "atm.elevation.location=" + elevation_location + "\n" +
                        "atm.watermask.location=" + appliedwatermask + "\n" +
                         aot_string +
                        "atm.ozone.location=" + ozone_location + "\n" +
                         simec_string +
                        "atm.watervapor.location=" + watervapor_location + "\n" +
                        "atm.output.location=" +  surface_reflectance_output + "\n" 
                        "atm.radiance.bands=" + bands_final
                         )
    

    if context["keep_land"] == "false":

        #dark correction
        #=========================================================================
        context.enter_stage("Dark Water Correction")
        #=========================================================================

        darkwater_corrected_output = location + "dark_corrected_surface_reflectance.tif"

        context.invoke_ac_runner_check(

                            "[darkwater correction]\n"
                            "reflectance.location=" + surface_reflectance_output + "\n" +
                            "output.location=" + darkwater_corrected_output + "\n" +
                            "watermask.location=" + appliedwatermask + "\n" +
                            "dark.band.id=15\n" +
                            "dark.band.fallback.id=13\n" +
                            "adjacency.low.id=9\n" +  
                            "adjacency.high.id=10\n" 
                            )
    
        product = darkwater_corrected_output


    if context["projectoutput"] == "true":
        product = location + "product.tif"
        #=========================================================================
        context.enter_stage("Project Output")
        #=========================================================================

        context.invoke_project_gpt(
            "icor.reproject "
            "-SgeotiffProduct=" + darkwater_corrected_output + " "
            "-SoriginalProduct=" + str(path) + " "
            "-t " + product
            )

    
    #=========================================================================
    context.enter_stage("Convert to Product")
    #=========================================================================
    
    context.invoke_ac_runner_check(

                        "[convert]\n"
                        "convert.input.location=" + product + "\n" +
                        "convert.output.location=" + str(output_file) + "\n" +
                        "convert.bands=0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15\n" +
                        "convert.bandunits=w_sqmsrcm w_sqmsrcm w_sqmsrcm w_sqmsrcm w_sqmsrcm w_sqmsrcm w_sqmsrcm w_sqmsrcm w_sqmsrcm w_sqmsrcm w_sqmsrcm w_sqmsrcm w_sqmsrcm w_sqmsrcm w_sqmsrcm w_sqmsrcm\n" +
                        "convert.bandnames=Oa1 Oa2 Oa3 Oa4 Oa5 Oa6 Oa7 Oa8 Oa9 Oa10 Oa11 Oa12 Oa16 Oa17 Oa18 Oa21\n"  
                        "convert.wavelengths=400.00 412.50 442.50 490.00 510.00 560.00 620.00 665.00 673.75 681.25 708.75 753.75 778.75 865.00 885.00 1020.00\n"
                        )
    

    #add intermediates    
    context.add_keep_tmp(appliedwatermask)
    context.add_keep_tmp(cloudmask)
    
    

    #=========================================================================
    context.enter_stage("Remove intermediate files from filesystem")
    #========================================================================= 

    keep_tmp = False
    if context["keep_intermediate"] == "true":
        keep_tmp = True
    context.remove_tmp_files(str(dir),keep_tmp,str(output_folder))

    print "iCOR Atmospheric correction done for product : " + productname