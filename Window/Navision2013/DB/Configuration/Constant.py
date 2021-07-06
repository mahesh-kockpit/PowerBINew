from pyspark.sql.types import *
import datetime


#SPARK_MASTER = "spark://10.11.5.243:7077"
WEB_HDFS_API = "http://192.10.15.132:9870/webhdfs/v1"
SPARK_MASTER = "spark://192.10.15.132:7077"
HDFS_PATH = "hdfs://master:9000"
STAGE1_HDFS_RELATIVE_PATH = "/DataMart/Stage1"
STAGE1_PATH = HDFS_PATH + STAGE1_HDFS_RELATIVE_PATH
STAGE2_PATH = HDFS_PATH + "/DataMart/Stage2"
PARQUET_FILE_PATH = "{0}/{1}.parquet"
MnSt = 4
yr = 3
owmode = 'overwrite'
apmode = 'append'
#cdate = datetime.datetime.now()

class PostgresDbInfo:
    Host = "192.10.15.134"
    Port = "5432"
    PostgresDB = "kockpit_new"
    PostgresUrl = "jdbc:postgresql://" + Host + "/" + PostgresDB
    Configurl = "jdbc:postgresql://" + Host + "/Configurator_Linux"
    logsDbUrl = "jdbc:postgresql://" + Host + "/Logs_new"
    url = "jdbc:postgresql://192.10.15.134/"
    props = {"user":"postgres", "password":"admin", "driver": "org.postgresql.Driver"}

class ConnectionInfo:
    JDBC_PARAM = "jdbc"
    SQL_SERVER_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    SQL_URL="jdbc:sqlserver://{0}:{1};databaseName={2};user={3};password={4}"
    
class PowerBISync:
    LOGIN_URL = 'https://login.microsoftonline.com/common/oauth2/token'
    LOGIN_REQUEST_PARAMS = {'grant_type': 'password',
         'username': 'ankit.kumar@teamcomputerspvtltd.onmicrosoft.com',
         'password': 'Cloud@@1',
         'client_id': 'f2228330-3a89-4050-a936-a366add4b5d4',
         'client_secret': '32z~E.xUXfF-8Ac7.ADqg0tT8bxI~_I~ny',
         'resource': 'https://analysis.windows.net/powerbi/api',
         'prompy': 'admin_consent'}
    WORKSPACE_ID = '941d5847-c431-4f65-94c0-f6e5437b5de1'   #Kockpit_Datamart
    GET_WORKSPACE_DATASET = 'https://api.powerbi.com/v1.0/myorg/groups/' + WORKSPACE_ID + '/datasets'
    REFRESH_WORKSPACE_DATASETS = 'https://api.powerbi.com/v1.0/myorg/groups/' + WORKSPACE_ID + '/datasets/{0}/refreshes'
    REFRESH_NOTIFY_OPTION = {'notifyOption': 'MailOnFailure'}

class Logger: 
    def __init__(self): 
        self._startTime = datetime.datetime.now()
        self._endTimeStr = None
        self._executionTime = None
        self._dateLog = self._startTime.strftime('%Y-%m-%d')
        self._startTimeStr = self._startTime.strftime('%H:%M:%S') 
        self._schema = StructType([
            StructField('Date',StringType(),True),
            StructField('Start_Time',StringType(),True),
            StructField('End_Time', StringType(),True),
            StructField('Run_Time',StringType(),True),
            StructField('File_Name',StringType(),True),
            StructField('DB',StringType(),True),
            StructField('EN', StringType(),True),
            StructField('Status',StringType(),True),
            StructField('Log_Status',StringType(),True),
            StructField('ErrorLineNo.',StringType(),True),
            StructField('Rows',IntegerType(),True),
            StructField('Columns',IntegerType(),True),
            StructField('Source',StringType(),True)
        ])

    def getSchema(self):
        return self._schema
    
    def endExecution(self):
        end_time = datetime.datetime.now()
        self._endTimeStr = end_time.strftime('%H:%M:%S')
        etime = str(end_time-self._startTime)
        self._executionTime = etime.split('.')[0]
    
    def getSuccessLoggedRecord(self, fileName, dbName, entityName, rowsCount, columnsCount, source):
        return  [{'Date': self._dateLog,
                    'Start_Time': self._startTimeStr,
                    'End_Time': self._endTimeStr,
                    'Run_Time': self._executionTime,
                    'File_Name': fileName,
                    'DB': dbName,
                    'EN': entityName,
                    'Status': 'Completed',
                    'Log_Status': 'Completed', 
                    'ErrorLineNo.': 'NA', 
                    'Rows': rowsCount, 
                    'Columns': columnsCount,
                    'Source': source
                }]

    def getErrorLoggedRecord(self, fileName, dbName, entityName, exception, errorLineNo, source):
        return  [{'Date': self._dateLog,
                    'Start_Time': self._startTimeStr,
                    'End_Time': self._endTimeStr,
                    'Run_Time': self._executionTime,
                    'File_Name': fileName,
                    'DB': dbName,
                    'EN': entityName,
                    'Status': 'Failed',
                    'Log_Status': str(exception),
                    'ErrorLineNo.': str(errorLineNo),
                    'Rows': 0,
                    'Columns': 0,
                    'Source': source
                }]