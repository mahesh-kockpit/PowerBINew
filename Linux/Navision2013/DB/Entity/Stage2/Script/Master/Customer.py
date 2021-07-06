
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import csv, io,re,keyring,os,datetime
from datetime import timedelta, date
from pyspark.sql.functions import col,max as max_,concat,concat_ws,year,when,month,to_date,lit,quarter,expr,sum,datediff,length,ltrim
import datetime,time,sys,calendar
from pyspark.sql.types import *
import re
from os.path import dirname, join, abspath
from pyspark.sql.concatenate import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import pyspark.sql.functions as F
from builtins import str
import pandas as pd
import keyring,os 
import traceback
import os,sys,subprocess,keyring
from os.path import dirname, join, abspath
from distutils.command.check import check
import datetime as dt
# 
#helpersDir = "D:\AMIT_KUMAR\Kockpit\DB1"
helpersDir = abspath(join(join(dirname(__file__), '..'),'..','..'))
print(helpersDir)
sys.path.insert(0, helpersDir)
st = dt.datetime.now()

from Configuration.AppConfig import *
from Configuration.Constants import *
from Configuration.udf import *
#from Configuration.DBInfo import *

conf = SparkConf().setMaster("local[8]").setAppName("Customer")
sc = SparkContext(conf = conf)
sqlCtx = SQLContext(sc)
spark = sqlCtx.sparkSession



class PostgresDbInfo:
    Host = "192.10.15.134"
    Port = "5432"
    PostgresDB = "kockpit_new"
    PostgresUrl = "jdbc:postgresql://" + Host + "/" +PostgresDB
    Configurl = "jdbc:postgresql://" + Host + "/Configurator_Linux"
    logsDbUrl = "jdbc:postgresql://" + Host + "/Logs_new"
    url = "jdbc:postgresql://192.10.15.134/"
    props = {"user":"postgres", "password":"admin", "driver": "org.postgresql.Driver"}
    

class ConnectionInfo:
    JDBC_PARAM = "jdbc"
    SQL_SERVER_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    SQL_URL="jdbc:sqlserver://{0}:{1};databaseName={2};user={3};password={4}"
sqlurl=ConnectionInfo.SQL_URL.format(config["SourceDBConnection"]['url'], str(config["SourceDBConnection"]["port"]), config["SourceDBConnection"]['databaseName'], config["SourceDBConnection"]['userName'], config["SourceDBConnection"]['password'],config["SourceDBConnection"]['dbtable'])
def RENAME(df,columns):
    """
        This Function Will Return a DataFrame having Columns Renamed
        Syntax for calling function: df = udf.RENAME(df, columns)
        Where columns is a dictionary containing old and new names e.g.:
    columns = {'old_name1': 'new_name1', 'old_name2': 'new_name2'}
        """
    if isinstance(columns, dict):
        for old_name, new_name in columns.items():
            df = df.withColumnRenamed(old_name, new_name)
        return df
    else:
        raise ValueError("'columns' should be a dict, like {'old_name_1':'new_name_1', 'old_name_2':'new_name_2'}")

'''
from pathlib import Path
p = list(Path(os.getcwd()).parts)
helpersDir = '/' + p[1] + '/' + p[2] #root/KockpitStudio'
'''
# #helpersDir = "D:\AMIT_KUMAR\Kockpit\DB1"
                               

from Configuration.AppConfig import *


for Table in config["TablesToIngest"]:
    
    table_name = 'Team Computers Pvt Ltd$' + Table['Table']
    #print(table_name) 

CUST_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "Customer") 
table_rename = next (table for table in config["TablesToRename"] if table["Table"] == "Customer") 
columns = table_rename["Columns"][0]    

        
try:
    CUST_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "Customer")
    
    
    for entityObj in config["DbEntities"]:
        logger = Logger()
        entityLocation = entityObj["Location"]
        
        
        DBName = entityLocation[:3]
        EntityName = entityLocation[-2:]
        
        postgresUrl = PostgresDbInfo.PostgresUrl
#         print(postgresUrl)
        
        
        #Path = "D:/AMIT_KUMAR/Kockpit/DB1/E2/Stage2/ParquetData"
        Path = helpersDir+"/"+"ParquetData"
        
        finalDF=spark.read.parquet(Path+"/"+table_name)
        finalDf.show()
        exit()
        finalDF = finalDF.withColumn('DB',lit(DBName))\
                    .withColumn('Entity',lit(EntityName))
        finalDF = finalDF.withColumn('Link Customer Key',concat(finalDF["DB"],lit('|'),finalDF["Entity"],lit('|'),finalDF["No_"]))
        finalDF.printSchema()
        
        finalDF = RENAME(finalDF,columns)
        #finalDF.printSchema()
        result_df = finalDF.select([F.col(col).alias(col.replace(" ","")) for col in finalDF.columns])
        result_df = result_df.select([F.col(col).alias(col.replace("(","")) for col in result_df.columns])
        result_df = result_df.select([F.col(col).alias(col.replace(")","")) for col in result_df.columns])
        result_df.coalesce(1).write.mode("overwrite").parquet("D:\AMIT_KUMAR\Kockpit\DB1\E2\Stage2\ParquetData\\"+table_name)
        result_df.show()
        
        result_df.write.jdbc(url=postgresUrl, table="DB1E1.Customer_new", mode=owmode, properties=PostgresDbInfo.props)
        logger.endExecution()
        try:
            IDEorBatch = sys.argv[1]
        except Exception as e :
            IDEorBatch = "IDLE"

        log_dict = logger.getSuccessLoggedRecord("Customer", DBName, EntityName, finalDF.count(), len(finalDF.columns), IDEorBatch)
        log_df = spark.createDataFrame(log_dict, logger.getSchema())
        log_df.write.jdbc(url=PostgresDbInfo.logsDbUrl, table="logtable_new", mode='append', properties=PostgresDbInfo.props)
            
            
            
            
            
            
            
except Exception as ex:
    exc_type,exc_value,exc_traceback=sys.exc_info()
    print("Error:",ex)
    print("type - "+str(exc_type))
    print("File - "+exc_traceback.tb_frame.f_code.co_filename)
    print("Error Line No. - "+str(exc_traceback.tb_lineno))
    ex = str(ex)
    logger.endExecution()
    try:
        IDEorBatch = sys.argv[1]
    except Exception as e :
        IDEorBatch = "IDLE"
        
    log_dict = logger.getErrorLoggedRecord('Customer', '', '', str(ex), exc_traceback.tb_lineno, IDEorBatch)
    log_df = spark.createDataFrame(log_dict, logger.getSchema())
    log_df.write.jdbc(url=PostgresDbInfo.logsDbUrl, table="logtable_new", mode='append', properties=PostgresDbInfo.props)        
print('masters_customer completed: ' + str((dt.datetime.now()-st).total_seconds()))