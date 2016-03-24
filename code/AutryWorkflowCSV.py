#!/usr/bin/env python

from pyspark import SparkContext
from pyspark.sql.functions import explode
from py4j.java_gateway import java_import
from workflow import Workflow
from sys import argv
import json
import sys
from digSparkUtil.fileUtil import FileUtil



if __name__ == "__main__":
    
    def mapFunc(x): 
        f_dict  = {}
        f_dict["@id"] = x['uri']
        f_dict["@type"] = "http://schema.org/Person"
        try:
            f_dict["schema:name"] = x['label']
        except KeyError:
            pass
        return f_dict
        

    sc = SparkContext(appName="TEST")

    java_import(sc._jvm, "edu.isi.karma")

    inputFilename = argv[1]
    outputFilename = argv[2]


    fileUtil = FileUtil(sc)
    workflow = Workflow(sc)
    contextUrl = "https://raw.githubusercontent.com/american-art/aac-alignment/master/karma-context.json"

    #1. Read the input

    #test big file
    inputRDD = workflow.batch_read_csv(inputFilename).partitionBy(1)

    #test small file
    # inputRDD = workflow.batch_read_csv(inputFilename)


    #2. Apply the karma Model
    outputRDD = workflow.run_karma(inputRDD,
                                   "https://raw.githubusercontent.com/american-art/autry/master/AutryMakers/AutryMakers-model.ttl",
                                   "http://dig.isi.edu/AutryMakers/",
                                   "http://www.cidoc-crm.org/cidoc-crm/E22_Man-Made_Object1",
                                   "https://raw.githubusercontent.com/american-art/aac-alignment/master/karma-context.json",
                                   data_type="csv",
                                   additional_settings={"karma.input.delimiter":","})

    #3. Save the output
    # fileUtil.save_file(outputRDD, outputFilename, "text", "json")

    reducedRDD = workflow.reduce_rdds(outputRDD)

    reducedRDD.persist()
    types = [
        {"name": "E82_Actor_Appellation", "uri": "http://www.cidoc-crm.org/cidoc-crm/E82_Actor_Appellation"}
    ]
    frames = [
        {"name": "AutryMakers", "url": "https://raw.githubusercontent.com/american-art/aac-alignment/master/frames/autryMakers.json-ld"}
    ]

    context = workflow.read_json_file(contextUrl)
    framer_output = workflow.apply_framer(reducedRDD, types, frames)
    for frame_name in framer_output:
        outputRDD = workflow.apply_context(framer_output[frame_name], context, contextUrl)
        #apply mapValues function
        outputRDD_after = outputRDD.mapValues(mapFunc)

        if not outputRDD_after.isEmpty():
            fileUtil.save_file(outputRDD_after, outputFilename + "/" + frame_name, 'text', 'json')
            print "Save to:", ("---" + frame_name)
            # workflow.save_rdd_to_es(outputRDD, es_server, es_port, es_index + "/" + frame_name)

