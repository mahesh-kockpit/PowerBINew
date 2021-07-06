'''
Created on 7 Jan 2020
@author: Jaydip
'''
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import csv, io,re,keyring,os,datetime
from datetime import timedelta, date
from pyspark.sql.functions import col,max as max_,concat,concat_ws,year,when,month,to_date,lit,quarter,expr,sum,datediff,length,ltrim
import datetime,time,sys,calendar
from pyspark.sql.types import *
import re
from builtins import str
import pandas as pd
import traceback
import os,sys,subprocess
from os.path import dirname, join, abspath
from distutils.command.check import check
import datetime as dt

helpersDir = '/home/padmin/KockpitStudio'
sys.path.insert(0, helpersDir)
from ConfigurationFiles.AppConfig import *
from Helpers.Constants import *
from Helpers.udf import *

def masters_salesPerson(sqlCtx, spark):
    st = dt.datetime.now()
    logger = Logger()

    try:
        SP_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "Salesperson_Purchaser") 
        table_rename = next (table for table in config["TablesToRename"] if table["Table"] == "Salesperson")
        columns = table_rename["Columns"][0]
        for entityObj in config["DbEntities"]:
            logger = Logger()
            entityLocation = entityObj["Location"]
            DBName = entityLocation[:3]
            EntityName = entityLocation[-2:]
            hdfspath = STAGE1_PATH + "/" + entityLocation
            postgresUrl = PostgresDbInfo.url.format(entityLocation)
            
            SalesPerson = ToDFWitoutPrefix(sqlCtx, hdfspath, SP_Entity,True)
            
            RSM = SalesPerson.select('Code','Name')\
                    .withColumnRenamed('Name','RSM_Name').withColumnRenamed('Code','RSM')
                    
            SalesPerson = SalesPerson.join(RSM,'RSM','left')
                                    
            BUHead = SalesPerson.select('Code','Name')\
                            .withColumnRenamed('Name','BUHead_Name').withColumnRenamed('Code','BUHead')
                            
            finalDF = SalesPerson.join(BUHead,'BUHead','left')
            finalDF.printSchema()
            finalDF = RENAME(finalDF,columns)
            finalDF.printSchema()
            finalDF = finalDF.withColumn('DB',lit(DBName))\
                    .withColumn('Entity',lit(EntityName))
            finalDF = finalDF.withColumn('Link SalesPerson Key',concat(finalDF["DB"],lit('|'),finalDF["Entity"],lit('|'),finalDF["Link SalesPerson"]))
            finalDF.printSchema()
            
            finalDF.cache()
            finalDF.write.jdbc(url=postgresUrl, table="Masters.Salesperson", mode='overwrite', properties=PostgresDbInfo.props)#PostgresDbInfo.props

            logger.endExecution()
            
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"

            log_dict = logger.getSuccessLoggedRecord("Salesperson", DBName, EntityName, finalDF.count(), len(finalDF.columns), IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.logsDbUrl, table="logtable", mode='append', properties=PostgresDbInfo.props)
        
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
        
        log_dict = logger.getErrorLoggedRecord('Salesperson', '', '', ex, exc_traceback.tb_lineno, IDEorBatch)
        log_df = spark.createDataFrame(log_dict, logger.getSchema())
        log_df.write.jdbc(url=PostgresDbInfo.logsDbUrl, table="logtable", mode='append', properties=PostgresDbInfo.props)
    print('masters_salesPerson completed: ' + str((dt.datetime.now()-st).total_seconds()))

if __name__ == "__main__":
    sqlCtx, spark = getSparkConfig(SPARK_MASTER, "Stage2:Salesperson")
    masters_salesPerson(sqlCtx, spark)