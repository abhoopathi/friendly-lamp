from pyspark.sql import SQLContext
import pandas as pd
import pymysql
import warnings
warnings.filterwarnings("ignore")
from datetime import datetime
import config


from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import os
import pandas as pd

from pyspark.sql import SparkSession
import pymysql

def connect_to_mysql():
    connection = pymysql.connect(host = config.db_host,
                            port= config.db_port,
                            user= config.db_user,
                            password= config.db_pass,
                            db= config.db_name,
                            charset='utf8',
                            cursorclass=pymysql.cursors.DictCursor)
    return connection

conn=connect_to_mysql()


# Checking whether the reference data is available in db or not , if no then creating it
with conn.cursor() as cursor:
        # Read a  record
        sql = "SHOW TABLES LIKE 'reference_df'" 
        cursor.execute(sql)
        result = (cursor.fetchall())

if result:
    print(' -I- reference data already created ')

else:
    
    # initialise sparkContext
    spark1 = SparkSession.builder \
        .master(config.sp_master) \
        .appName(config.sp_appname) \
        .config('spark.executor.memory', config.sp_memory) \
        .config("spark.cores.max", config.sp_cores) \
        .getOrCreate()


    sc = spark1.sparkContext
    
    # using SQLContext to read parquet file

    sqlContext = SQLContext(sc)

    df = sqlContext.read.parquet(config.proj_path+'/datas/appid_datapoint_parquet1')

    from sqlalchemy import create_engine
    engine = create_engine(str("mysql+pymysql://"+config.db_user+":"+config.db_pass+"@"+config.db_host+":"+str(config.db_port)+"/"+config.db_name))


    from pyspark.sql.functions import when

    q1 = datetime.now()
    df1 = df[(df.app_rsp_time !=0)]
    
    df_t = df1.registerTempTable('dummy')
    df_t = sqlContext.sql('select count(*) as count, source , application, target_address  from dummy group by source, application, target_address')

    df_t= df_t.withColumn('count_flag', when(df_t['count']>config.limit,1).otherwise(0))
    df_t = df_t[df_t.count_flag==1]
    
    # fetching the  source which is to be filtered from filter_db table
    with conn.cursor() as cursor:
        # Read a  record
        sql = "select * from filter_db" 
        cursor.execute(sql)
        so_result = pd.DataFrame(cursor.fetchall())
    
    #filtering
    from pyspark.sql.functions import col
    #print(so_result)
    s_filter = list(so_result.source)
    df_t = df_t.filter(~col('source').isin(s_filter))
    #df_t = df_t[df_t.source!='134.141.5.104']
    df2 = df_t.toPandas()

    q2 = datetime.now()

    print('time to refernce data prepration is ',str(q2-q1))
    df2 = df2.iloc[0:100,:]
    df2.to_sql(con=engine, name='reference_df', if_exists='replace')
          
conn.close()