#!/usr/bin/env python2

import argparse
import logging
import os
import os.path
import sys
import traceback
import atexit
import time
import subprocess
import ConfigParser
import icor.context
import icor.landsat8
import icor.sentinel2
import icor.sentinel3


class Range(object):

    def __init__(self, start, end):
        self.start = start
        self.end = end

    def __eq__(self, other):
        return self.start <= float(other) <= self.end

    def __repr__(self):
        return "{0} - {1}".format(self.start,self.end)


def process_product(context, product):
    try:
        working_folder = context["working_dir"]
        if context["instrument"] == "landsat8":
            if context["workflow"] == "simec":
                icor.landsat8.process_tgz(context, product,working_folder)
            else:
                raise "Unknown 'instrument'"
        elif context["instrument"] == "sentinel2":
            if context["workflow"] == "simec":

                icor.sentinel2.process_saf(context, product,working_folder)
            else:
                raise "Unknown 'instrument'"

        elif context["instrument"] == "sentinel3":
            if context["workflow"] == "simec":

                icor.sentinel3.process(context, product,working_folder)
            else:
                raise "Unknown 'instrument'"
        else:
            raise "Unknown 'workflow'"
    except:
        logging.error(traceback.format_exc())
        raise


if __name__ == "__main__":

    ap = argparse.ArgumentParser(description="iCOR Toolbox")

    def _valid_file(arg):
        if not os.path.isfile(arg):
            ap.error("The path %s is not a file" % arg)
        elif os.path.splitext(arg)[1].lower() != '.xml':
            ap.error("Product must be an XML file.")
        else:
            return arg

    ap.add_argument("-v", "--verbose", action="store_true",  help="Increase output verbosity")

    ap.add_argument("--data_type", choices=['S2','L8','S3'], metavar="DATATYPE", help="Sentinel 2, Landsat-8 or Sentinel 3. Values : S2, L8 or S3", required=True )
    ap.add_argument("--cloud_low_band",choices=['B01','B02','B03','B04','B05','B06','B07','B08','B8A','B09','B10','B11','B12',
                                                'B13','B14','B15','B16','B17','B19','B19','B20','B21'], metavar="CLDLOWBAND",  help="Band to apply cloud low threshold (zero based)." , required=True)
    ap.add_argument("--water_band", choices=['B01','B02','B03','B04','B05','B06','B07','B08','B8A','B09','B10','B11','B12',
                                             'B13','B14','B15','B16','B17','B18','B19','B20','B21'],metavar="WATERBAND",  help="Water detection band id (zero based)." , required=True)

    ap.add_argument("--cloud_average_threshold" , choices=[Range(0.0,1.0)] ,metavar="CLDAVGTHRSH",  help="Upper threshold with average in the visual bands to be detected as cloud." , required=True)
    ap.add_argument("--cloud_low_threshold" , choices=[Range(0.0,1.0)] ,metavar="CLDLOWTHRSH",  help="Low band threshold to be detected as cloud." , required=True)
    ap.add_argument("--aot_window_size", type=int, metavar="AOTWINDOW",  help="Square window size in pixels to perform aot estimation" , required=True)

    ap.add_argument("--water_threshold", choices=[Range(0.0,1.0)], metavar="WATERTHRSHD",  help="Water detection threshold." , required=True)

    ap.add_argument("--cirrus", choices=['true','false'] ,metavar="CIRRUS",  help="Use cirrus band for cloud detection. Value : true or false" , required=False)
    ap.add_argument("--aot", choices=['true','false'],metavar="AOT",  help="Apply AOT retrieval algorithm. Value : true or false", required=True )
    ap.add_argument("--simec", choices=['true','false'],metavar="SIMEC",  help="Apply adjacency correction. Value : true or false", required=True )
    ap.add_argument("--watervapor", choices=['true','false'],metavar="WATERVAPOR",  help="Apply watervapor estimation. Value : true or false", required=False )
    ap.add_argument("--bg_window", type=int, metavar="BG_WINDOW", help="Default background window size", required=True )
    ap.add_argument("--cirrus_threshold", choices=[Range(0.0,1.0)] , metavar="CIRRUSTHRESHOLD",  help="Cloud mask threshold value", required=False )
    ap.add_argument("--aot_override", choices=[Range(0.0,1.2)], metavar="AOT_OVERRIDE",  help="AOT override values", required=True )
    ap.add_argument("--ozone", choices=['true','false'],metavar="OZONE",  help="Use product ozone values. Value : true or false", required=False )
    ap.add_argument("--ozone_override" , choices=[Range(0.25,0.5)], metavar="OZONE_OVERRIDE",  help="OZONE override values", required=True )
    ap.add_argument("--wv_override", choices=[Range(0.0,5.0)], metavar="WV_OVERRIDE",  help="WATERVAPOR override value", required=True )

    ap.add_argument("--output_file", metavar="OUTPUTDIR", help="Location of output file", required=True )
    ap.add_argument("--working_folder", metavar="WORKINGDIR", help="Location of intermediate date files", required=False )

    ap.add_argument("--keep_intermediate", choices=['true','false'], metavar="WORKINGDIR", help="Keep intermediate files. Value : true or false", required=True )

    #S3 specific
    ap.add_argument("--inlandwater", choices=['true','false'], metavar="INLAND_WATER",  help="", required=False )
    ap.add_argument("--productwatermask", choices=['true','false'], metavar="INLAND_WATER",  help="", required=False )
    ap.add_argument("--sensor", metavar="SENSORNAME", help="Sentinel3 A or B sensor", required=False )
    ap.add_argument("--apply_svc_gains", metavar="APPLY_SVC_GAINS", help="SVC gains apply or not", required=False )
    ap.add_argument("--keep_land", metavar="KEEP_LAND", help="Keep all land pixels", required=False )
    ap.add_argument("--keep_water", metavar="KEEP_WATER", help="Keep all water pixels", required=False )
    ap.add_argument("--project", metavar="PROJECT", help="Project Output", required=False )

    ap.add_argument("product", metavar="PROD", nargs="+", type=_valid_file, help="An input product archive")

    print "parsing arguments"
    args = ap.parse_args()
    print "done"

    if args.verbose:
        logging.basicConfig(format="%(asctime)s [%(levelname)s]: %(message)s", level=logging.DEBUG)
    else:
        logging.basicConfig(format="%(message)s", level=logging.INFO)


    conf = ConfigParser.SafeConfigParser()
    try:
        icor_dir = str(os.environ['ICOR_DIR'])
    except Exception:
        raise Exception("environment variable ICOR_DIR not set")

    print "icor installation folder " + icor_dir

    conf.set("DEFAULT","install_dir",icor_dir)



    if args.data_type == "L8":
        print "running icor for Landsat8 ..." + icor_dir
        print "reading config " + icor_dir + "/src/config/local_landsat8_simec.ini"
        conf.read( icor_dir + "/src/config/local_landsat8_simec.ini")
    elif args.data_type == "S2":
        print "running icor for Sentinel2 ..."
        print "reading config " + icor_dir + "/src/config/local_sentinel2_simec.ini"
        conf.read( icor_dir +  "/src/config/local_sentinel2_simec.ini")
    elif args.data_type == "S3":
        print "running icor for Sentinel3 ..." + icor_dir
        print "reading config " + icor_dir + "/src/config/local_sentinel3_simec.ini"
        conf.read( icor_dir +  "/src/config/local_sentinel3_simec.ini")

    args._get_kwargs()

    params = {}

    # convert to params for context
    context = icor.context.SimpleContext(params)

    params["aot"]                   = args.aot
    params["simec"]                 = args.simec
    params["watervapor"]            = args.watervapor
    params["bg_window"]             = args.bg_window

    params["aot_override"]          = args.aot_override
    params["ozone_override"]        = args.ozone_override
    params["watervapor_override"]   = args.wv_override

    if args.data_type == "S3":
        params["inlandwater"]       = args.inlandwater
        params["productwatermask"]  = args.productwatermask
        params["sensorname"]        = args.sensor
        params["apply_svc"]         = args.apply_svc_gains
        params["keep_land"]         = args.keep_land
        params["keep_water"]         = args.keep_water
        params["ozone"]             = args.ozone
        params["projectoutput"]     =args.project

    if args.data_type != "S3":
        params["cirrus"]                = args.cirrus
        params["cirrus_threshold"]      = args.cirrus_threshold

    root, ext = os.path.splitext(args.output_file)
    if os.path.isdir(args.output_file):
        params["output_file"] = os.path.abspath(os.path.join(args.output_file, 'out.tif'))
    elif ext != ".tif":
        params["output_file"] = os.path.abspath(str(root) + ".tif")
    else:
        params["output_file"] = os.path.abspath(args.output_file)

    print "output path = " + params["output_file"]

    params["low_band"]              = args.cloud_low_band
    params["average_threshold"]     = args.cloud_average_threshold
    params["low_threshold"]         = args.cloud_low_threshold

    params["aot_window_size"]       = args.aot_window_size

    #water detection algorithm
    params["water_band"]            = args.water_band
    params["water_threshold"]       = args.water_threshold
    params["keep_intermediate"]     = args.keep_intermediate

    if args.working_folder:
        params["working_dir"]           = args.working_folder
    else:
        params["working_dir"]           = context.make_temp_folder()
    print "iCOR temp folder : "  + str(params["working_dir"])

    for param, value in conf.items("DEFAULT"):
        params[param] = value
    print str(param) + " = " + str(value)

    for section in conf.sections():
        for param, value in conf.items(section):
            params[section + "_" + param] = value

    for product in args.product:
        process_product(context,product)
