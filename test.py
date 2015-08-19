#!/usr/bin/python
# Usage: spark-submit --jars /usr/local/spark/lib/mongo-hadoop-core-1.4.0.jar,/usr/local/spark/lib/mongo-java-driver-3.0.2.jar [script] [datasource] [nodeId] [timeStamp] [query]


import sys
import re
import datetime
import sys
import os
from pyspark.sql import Row
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import pyspark
from __builtin__ import ValueError
from decimal import Decimal
from itertools import groupby

dataSource = sys.argv[1];
nodeId = sys.argv[2];
timeStamp = sys.argv[3];
query = sys.argv[4];

f = open('/home/wai/Data/queries.log', 'a')
f.write(str(sys.argv) + '\n')
f.close()

result = [{
    "NodeId": nodeId,
    "TimeStamp": timeStamp,
    "Query": query,
    "Result": "dummy"
}]

sc = SparkContext("local", "Test App")
sqlContext = SQLContext(sc)
df = sqlContext.createDataFrame(result)

# configuration for output to MongoDB
config = {
    "mongo.input.uri": "mongodb://localhost:27017/GDO_Apps_TimeV.Query_Queue", 
    "mongo.output.uri": "mongodb://localhost:27017/GDO_Apps_TimeV.Query_Results" 
}
outputFormatClassName = "com.mongodb.hadoop.MongoOutputFormat"
df.rdd.map(lambda row: (None, row.asDict())).saveAsNewAPIHadoopFile("file:///placeholder", outputFormatClassName, None, None, None, None, config)
