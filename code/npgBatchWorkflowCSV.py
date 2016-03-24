#!/usr/bin/env python
from pyspark import SparkContext
from py4j.java_gateway import java_import
from workflow import Workflow
from sys import argv
import json
import sys
from digSparkUtil.fileUtil import FileUtil
import xlrd
import csv
import glob
import urllib2
import zipfile
import os
from git import Repo

# Executed as:
#
# ./makeSpark.sh
#
#
# rm -rf karma-out-test; spark-submit \
#       --archives karma.zip \
#       --py-files lib/python-lib.zip \
#       --driver-class-path /Users/ChloeDing/Desktop/ISI/Web-Karma/karma-spark/target/karma-spark-0.0.1-SNAPSHOT-shaded.jar \
#       npgBatchWorkflowCSV.py paramList karma-out-test


def zip(src, dst):
    with zipfile.ZipFile(dst, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(src, os.path.basename(src))


def excel_to_csv(file_name):
    wb = xlrd.open_workbook(file_name)
    sh = wb.sheet_by_name('Sheet1')
    csv_file_name = file_name.rsplit(".",1)[0] + ".csv"
    f_csv = open(csv_file_name, 'w+')
    wr = csv.writer(f_csv)
    for rownum in xrange(sh.nrows):
        wr.writerow([unicode(r).encode("utf8") for r in sh.row_values(rownum)])
    f_csv.close()
    return csv_file_name


def download_file(data_file_URL):
    url = data_file_URL
    file_name = url.split('/')[-1]
    u = urllib2.urlopen(url)
    f = open(file_name, 'wb')
    meta = u.info()
    file_size = int(meta.getheaders("Content-Length")[0])
    print "Downloading: %s Bytes: %s" % (file_name, file_size)
    file_size_dl = 0
    block_sz = 8192
    while True:
        buffer = u.read(block_sz)
        if not buffer:
            break
        file_size_dl += len(buffer)
        f.write(buffer)
        status = r"%10d  [%3.2f%%]" % (file_size_dl, file_size_dl * 100. / file_size)
        status = status + chr(8)*(len(status)+1)
        print status,
    f.close()
    print "Download Done!"
    # transfer excel to csv file
    print "Transfer Excel to CSV"
    csv_file_name = excel_to_csv(file_name)
    print "Transfer " + csv_file_name + " Done!"

    return csv_file_name

def get_sample_file(input_sample_file, output_sample_file):
    num_lines = sum(1 for line in open(input_sample_file))
    N = 100 if num_lines >= 100 else num_lines
    with open(output_sample_file, "w") as f_out:
        with open(input_sample_file, "r") as f_in:
            head = [next(f_in) for x in xrange(N)]
            f_out.write("".join(head))

def concate_file(input_sum_file, output_sum_file):
    with open(output_sum_file, "a+") as f_out:
        for fn in os.listdir(input_sum_file):
            if fn.startswith("part-") and os.path.getsize(input_sum_file + fn) > 0:
                with open(input_sum_file + fn, "r") as f_in:
                    # print fn
                    for line in f_in:
                        f_out.write(line)
            else:
                os.remove(input_sum_file + fn)


if __name__ == "__main__":
    sc = SparkContext(appName="TEST")

    java_import(sc._jvm, "edu.isi.karma")

    fileUtil = FileUtil(sc)
    workflow = Workflow(sc)

    inputFilename = argv[1]
    outputFilename = argv[2]

    with open(inputFilename) as fin:
        for line in fin:
            line = line.rstrip()
            params = line.split("\t")
            data_file_URL = str(params[0])
            num_partitions = int(params[1])
            model_file_URL= str(params[2])
            base = str(params[3])
            root = str(params[4])
            context = str(params[5])
            output_folder = str(params[6])
            output_zip_path = str(params[7])

            #0. Download data file
            dataFileName = download_file(data_file_URL)

            #1. Read the input
            inputRDD = workflow.batch_read_csv(dataFileName).partitionBy(num_partitions)

            #2. Apply the karma Model
            outputRDD = workflow.run_karma(inputRDD,
                                            model_file_URL,
                                            base,
                                            root,
                                            context,
                            data_type="csv",
                            additional_settings={"karma.input.delimiter":",", "karma.output.format": "n3"})

            #3. Save the output
            outputPath = outputFilename + "/" + output_folder
            outputRDD.map(lambda x: x[1]).saveAsTextFile(outputPath)
            print "Successfully apply karma!"

            #4. Concate data files
            input_sum_file = outputFilename + "/" + output_folder + "/"
            output_sum_file = outputFilename + "/" + output_folder + ".n3"
            concate_file(input_sum_file, output_sum_file)
            print "Successfully generate whole data file!"

            #5. Get the sample file
            input_sample_file = output_sum_file
            output_sample_file = output_zip_path + "/" + output_folder + "-sample.n3"
            get_sample_file(input_sample_file, output_sample_file)
            print "Successfully generate sample file!"

            #6. Zip Output Files
            output_zip_file = output_zip_path + "/" + output_folder + ".n3.zip"
            zip(output_sum_file, output_zip_file)
            print "Successfully zip files!"


            #7. Upload to Github
            repo = Repo("/Users/ChloeDing/Desktop/ISI/American_Art")
            index = repo.index
            index.add([output_zip_file, output_sample_file]) 
            index.commit("update %s.n3.zip and %s-sample.n3" % (output_folder, output_folder))
            o = repo.remotes.origin
            o.push()
            print "Successfully push files to Github!"
