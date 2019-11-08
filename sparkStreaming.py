#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Columbia ELEN E6776 Content Storage & Distribution

# Created by Sidharth Bambah

"""
This module is the spark streaming analysis process.


Usage:
    If used with dataproc:
        gcloud dataproc jobs submit pyspark --cluster <Cluster Name> twitterHTTPClient.py

    Create a dataset in BigQurey first using
        bq mk bigdata_sparkStreaming

    Remeber to replace the bucket with your own bucket name


Todo:
    1. hashtagCount: calculate accumulated hashtags count
    2. wordCount: calculate word count every 10 seconds
        the word you should track is listed below.
    3. save the result to google BigQuery

"""

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
import time
from datetime import datetime
import subprocess
import re
from google.cloud import bigquery
import os

os.mkdir(os.getcwd() + '/checkpoint_TwitterApp')
open(os.getcwd() + '/checkpoint_TwitterApp/temp', 'a').close()

# global variables
bucket = "big-data-columbia"
output_directory_hashtags = 'gs://{}/hadoop/tmp/bigquery/pyspark_output/hashtagsCount'.format(bucket)

# output table and columns name
output_dataset = 'cdn_dataset'
output_table_hashtags = 'hashtags'
columns_name_hashtags = ['hashtags', 'count']


# parameter
IP = 'localhost'    # ip port
PORT = 9001       # port

STREAMTIME = 1200          # time that the streaming process runs


# Helper functions
def saveToStorage(rdd, output_directory, columns_name, mode):
    """
    Save each RDD in this DStream to google storage
    Args:
        rdd: input rdd
        output_directory: output directory in google storage
        columns_name: columns name of dataframe
        mode: mode = "overwrite", overwirte the file
              mode = "append", append data to the end of file
    """
    if not rdd.isEmpty():
        (rdd.toDF( columns_name ) \
        .write.save(output_directory, format="json", mode=mode))


def saveToBigQuery(sc, output_dataset, output_table, directory):
    """
    Put temp streaming json files in google storage to google BigQuery
    and clean the output files in google storage
    """
    files = directory + '/part-*'
    subprocess.check_call(
        'bq load --source_format NEWLINE_DELIMITED_JSON '
        '--replace '
        '--autodetect '
        '{dataset}.{table} {files}'.format(
            dataset=output_dataset, table=output_table, files=files
        ).split())
    output_path = sc._jvm.org.apache.hadoop.fs.Path(directory)
    output_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(
        output_path, True)

def sum_tags(new_values, total_sum): 
    return sum(new_values) + (total_sum or 0)
    
def hashtagCount(words):
    """
    Calculate the accumulated hashtags count sum from the beginning of the stream
    and sort it by descending order of the count.
    Ignore case sensitivity when counting the hashtags:
        "#Ab" and "#ab" is considered to be a same hashtag
    You have to:
    1. Filter out the word that is hashtags.
       Hashtag usually start with "#" and followed by a serious of alphanumeric
    2. map (hashtag) to (hashtag, 1)
    3. sum the count of current DStream state and previous state
    4. transform unordered DStream to a ordered Dstream
    Hints:
        you may use regular expression to filter the words
        You can take a look at updateStateByKey and transform transformations
    Args:
        dstream(DStream): stream of real time tweets
    Returns:
        DStream Object with inner structure (hashtag, count)
    """
    
    # Filtering Hashtags
    filtered = words.filter(lambda x: x and len(x)>1 and x[0] == "#" and re.match('^[a-zA-Z0-9]*$',x[1:]))
    
    # Mapping and sending to lowercase
    mapped = filtered.map(lambda l: (l.lower(), 1))
    
    # Summing current and previous state
    summed = mapped.updateStateByKey(sum_tags)
    
    # Transforming from unordered to ordered
    result = summed.transform(lambda rdd: rdd.sortBy(lambda x: x[1], False))
    
    return result


if __name__ == '__main__':
    # Spark settings
    conf = SparkConf()
    conf.setMaster('local[2]')
    conf.setAppName("TwitterStreamApp")

    # create spark context with the above configuration
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    # create sql context, used for saving rdd
    sql_context = SQLContext(sc)

    # create the Streaming Context from the above spark context with batch interval size 5 seconds
    ssc = StreamingContext(sc, 5)
    # setting a checkpoint to allow RDD recovery
    ssc.checkpoint("/checkpoint_TwitterApp")
    
    # read data from port 9001
    dataStream = ssc.socketTextStream(IP, PORT)

    words = dataStream.flatMap(lambda line: line.split(" "))

    # calculate the accumulated hashtags count sum from the beginning of the stream
    topTags = hashtagCount(words)
    topTags.pprint()

    # save hashtags count to google storage
    # used to save to google BigQuery
    # topTags: only save the last DStream
    
    
    topTags.foreachRDD(lambda rdd:saveToStorage(rdd, output_directory_hashtags, columns_name_hashtags, "overwrite"))
    
    # start streaming process, wait for STREAMTIME and then stop.
    ssc.start()
    time.sleep(STREAMTIME)
    ssc.stop(stopSparkContext=False, stopGraceFully=True)

    # put the temp result in google storage to google BigQuery
    saveToBigQuery(sc, output_dataset, output_table_hashtags, output_directory_hashtags)
