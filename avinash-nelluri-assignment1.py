from __future__ import print_function

import os
import sys
from operator import add

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *


#Exception Handling and removing wrong datalines
def isfloat(value):
    try:
        float(value)
        return True
 
    except:
         return False

#Function - Cleaning
#For example, remove lines if they don’t have 16 values and 
# checking if the trip distance and fare amount is a float number
# checking if the trip duration is more than a minute, trip distance is more than 0 miles, 
# fare amount and total amount are more than 0 dollars
def correctRows(p):
    if(len(p)==17):
        if(isfloat(p[5]) and isfloat(p[11])):
            if(float(p[4])> 60 and float(p[5])>0 and float(p[11])> 0 and float(p[16])> 0 and float(p[16])< 10000):# added and float(p[16])<10000 for large data run just to filter out bad data.
                return p

#Main
if __name__ == "__main__":
        
    # Set your file path here
    path="C:\\Users\\avina\\Downloads\\"
    testFile= path + "taxi-data-sorted-small.csv"
    #testFile is used only in local context.

    sc = SparkContext(appName="Assignment-1")
    spark = SparkSession.builder.getOrCreate()
    sqlContext = SQLContext(sc)
        
    #testDataFrame = sqlContext.read.format('csv').options(header='false', inferSchema='true',  sep =",").load(testFile)#local
    testDataFrame = sqlContext.read.format('csv').options(header='false', inferSchema='true',  sep =",").load(sys.argv[1])#gcp
    #testDataFrame.show()
    
    testRDD = testDataFrame.rdd.map(tuple) # Used for Task 1
    
    # calling isfloat and correctRows functions to cleaning up data
    taxilinesCorrected = testRDD.filter(correctRows) # Used for Task 2
    print(taxilinesCorrected.take(1))

    #Task 1  
    top_10 = testRDD.map(lambda x: (x[0], x[1]))\
                .groupBy(lambda x: x[0]).mapValues(lambda v: set(v))\
                .mapValues(lambda v: len(v))\
                .sortBy(lambda x: x[1], ascending=False).take(10)    

    # Print the top 10
    print(top_10)
    
    # Save the top 10 to a text file
    top_10_rdd = sc.parallelize(top_10)
    #top_10_rdd.coalesce(1).saveAsTextFile("top_10_taxis")#local context
    top_10_rdd.coalesce(1).saveAsTextFile(sys.argv[2])#gcp context

 
    #Task 2    
    #Map each value to a tuple (value, 1)
    top_10_earners_per_min = taxilinesCorrected.map(lambda x: (x[1], x[4] / 60, x[16])).map(lambda x: (x[0], x[2] / x[1]))\
                    .map(lambda x: (x[0], (x[1], 1)))\
                    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))\
                    .mapValues(lambda x: x[0] / x[1])\
                    .sortBy(lambda x: x[1], ascending=False).take(10)
    print(top_10_earners_per_min)
    
    top_10_earners_rdd = sc.parallelize(top_10_earners_per_min)
    #top_10_earners_rdd.coalesce(1).saveAsTextFile("top_10_earners")#local context
    top_10_earners_rdd.coalesce(1).saveAsTextFile(sys.argv[3])#gcp context
    
    sc.stop()