import os
import sys
## added for error debugging of java_collections error
import py4j
os.environ['JAVA_HOME'] = "/usr/lib/jvm/java-8-oracle"
#os.environ['PYTHONPATH'] = "/root/anaconda3/lib/python3.6/site-packages/pyspark/python/lib/py4j-0.10.7-src.zip:/root/anaconda3/lib/python3.6/site-packages/pyspark/python:/root/anaconda3/lib/python3.6/python"
#os.environ['SPARK_HOME'] = "/root/anaconda3/lib/python3.6/site-packages/pyspark"
#sys.path.append("/usr/local/spark/spark-2.3.0-bin-hadoop2.7/python")
#sys.path.append("/usr/local/spark/spark-2.3.0-bin-hadoop2.7/python/lib")
import config
from datetime import datetime

#print(os.environ['PYTHONPATH'])
#print(os.environ['SPARK_HOME'])
#print(os.environ['JAVA_HOME'])
#import pyspark
#from pyspark.sql import SQLContext

#import parquet_file_creation
#import p3_final_beta
#import p10_final_beta


import schedule
import time

def job():
    t1 = datetime.now()
    #os.chdir(config.proj_path)

    print(os.getcwd())
    #os.system("python "+config.proj_path+"/parquet_file_creation.py")
    #os.system("python "+config.proj_path+"/source_code/p3_final_beta.py")
    #os.system("python "+config.proj_path+"/source_code/p10_final_beta.py")
    os.system("python "+config.proj_path+"/parquet_file_creation.cpython-36.pyc")
    os.system("python "+config.proj_path+"/p3_final_beta.cpython-36.pyc")
    os.system("python "+config.proj_path+"/p10_final_beta.cpython-36.pyc")
    #parquet_file_creation.main()
    #p3_final_beta.main()
    #p10_final_beta.main()


    t2 = datetime.now()

    print('Total time to run all scripts : ',str(t2-t1))


#schedule.every().day.at("00:00").do(job)
#schedule.every(1).minutes.do(job)

#while True:
    #schedule.run_pending()
    #time.sleep(60) # wait one minute
job()

