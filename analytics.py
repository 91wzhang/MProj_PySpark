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

sc = SparkContext("local", "GDO.Apps.TimeV")
sqlContext = SQLContext(sc)

TYPE_MAP = {
            'boolean': BooleanType(), 
            'double': DoubleType(), 
            'int': IntegerType(),
            'integer': IntegerType(),
            'long': LongType(),  
            'string': StringType(), 
            'timestamp': TimestampType()
           }

SCHEMA = [ ('node', 'long'), ('record_id', 'long'), ('user', 'string')\
         , ('created', 'timestamp'), ('seq_no', 'long'), ('node_time', 'long')\
         , ('system_time', 'long'), ('time_stamp', 'timestamp')\
         , ('cpu_power', 'double'), ('lpm_power', 'double')\
         , ('listen_power', 'double'), ('transmit_power', 'double')\
         , ('average_power', 'double'), ('temperature', 'double')\
         , ('battery_voltage', 'double'), ('battery_indicator', 'double')\
         , ('radio_intensity', 'double'), ('latency', 'double')\
         , ('humidity', 'double'), ('light1', 'double'), ('light2', 'double')\
         , ('best_neighbour_id', 'double'), ('best_neighbour_etx', 'double')\
         , ('sink', 'long'), ('is_duplicate', 'boolean'), ('hops', 'long')\
         ]

CAPTIONS = [ name for (name, _) in SCHEMA ]

SENSOR_TIME_FIELD_INDICE = [3, 7]                      

def parse_sensor_time(s):
    """ Convert sensor time format into a Python datetime object
    Args:
        s (str): date and time in sensor time format
    Returns:
        datetime: datetime object (ignore timezone for now)
    """
    return datetime.datetime(int(s[6:10]),
                             int(s[3:5]),
                             int(s[0:2]),
                             int(s[11:13]),
                             int(s[14:16]),
                             int(s[17:29]))
        
def parse_data_value(s, data_type, time_parser=parse_sensor_time):    
    if s is None or s == '':
        return None
    
    if data_type == 'boolean':
        return bool(s)
    elif data_type == 'double':
        return float(s) 
    elif data_type == 'int' or data_type == 'integer':
        return int(s)
    elif data_type == 'long':
        return long(s)
    elif data_type == 'timestamp':
        return time_parser(s)
    else:
        return s

def parseLogLine(logline, data_schema, spliter='\t'):
    """ Parse a line in the sensor log
    Args:
        logline (str): a line of values in the sensor log
    Returns:
        tuple: either a dictionary containing the parts of the sensor log and 1,
               or the original invalid log line and 0
    """
    vals = logline.strip().split(spliter) 
    field_cnt = len(data_schema)
    
    if len(vals) < field_cnt:
        vals = vals + [None] * (field_cnt - len(vals))
    elif len(vals) > field_cnt:
        return (logline, 0)        
        
    for idx in xrange(len(vals)): 
        try:
            vals[idx] = parse_data_value(vals[idx], data_schema[idx][1])
        except Exception:
            print 'failed to parse: ' + str(vals)                                          
    
    return vals

def parseLogs(logFilePath):
    
    """ Read and parse log file """
    data_file = sc.textFile(logFilePath)
    header = data_file.first()
    logs = data_file.filter(lambda log: log != header and log.strip())
    
    parsed_logs = (
                     logs
                    .map(lambda logline: parseLogLine(logline, SCHEMA))
                    .cache()
                  )            
        
    return parsed_logs

dataSource = sys.argv[1];
nodeId = sys.argv[2];
timeStamp = sys.argv[3];
query = sys.argv[4];

baseDir = os.path.join('/home/wai/Data')
inputPath = os.path.join(dataSource)
parquetPath = os.path.join(dataSource, '.parquet')
parquetFile = os.path.join(baseDir, parquetPath)

f = open('/home/wai/Data/queries.log', 'a')

try:
    with open(parquetFile) as log_df:
        f.write('read parquet cache | ' + query + '\n')
        pass
except IOError as e:
    f.write('no parquet cache | ' + query + '\n')
    logFile = os.path.join(baseDir, inputPath)
    parsed_logs = parseLogs(logFile)
    fields = [StructField(name, TYPE_MAP[data_type], True) for (name, data_type) in SCHEMA]
    df_schema = StructType(fields)
    log_df = sqlContext.createDataFrame(parsed_logs, df_schema)
    log_df.write.parquet(parquetFile)

f.close()
log_df.registerTempTable("log")

def takeMonthlyAverage(logDF, fields, time_field, num = 0):
    avg = logDF.groupBy(logDF[time_field].substr(1, 7)).avg(*fields)      
 
    if num == 0:
        return avg.collect()
    else:
        return avg.take(num)
        
def takeYearlyAverage(logDF, fields, time_field, num = 0):     
    avg = logDF.groupBy(logDF[time_field].substr(1, 4)).avg(*fields)      

    if num == 0:
        return avg.collect()
    else:
        return avg.take(num)  
    
# avg = takeYearlyAverage(log_df, ['humidity', 'light2', 'hops'], 'time_stamp', 3)
#avg = log_df.groupBy(log_df['time_stamp'].substr(1, 13)).agg({'hops':'avg', 'humidity':'avg'})    

result = sqlContext.sql(query)

# configuration for output to MongoDB
config = {
    "mongo.input.uri": "mongodb://localhost:27017/GDO_Apps_TimeV.Query_Queue", 
    "mongo.output.uri": "mongodb://localhost:27017/GDO_Apps_TimeV.Query_Results" 
}

outputFormatClassName = "com.mongodb.hadoop.MongoOutputFormat"
result.rdd.map(lambda row: (None, row.asDict())).saveAsNewAPIHadoopFile("file:///placeholder", outputFormatClassName, None, None, None, None, config)
