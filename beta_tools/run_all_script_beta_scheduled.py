import os
import sys
#os.environ['PYTHONPATH'] = "/root/anaconda3/lib/python3.6/python"
#os.environ['SPARK_HOME'] = "/usr/local/spark/spark-2.3.0-bin-hadoop2.7"
#sys.path.append("/usr/local/spark/spark-2.3.0-bin-hadoop2.7/python")
#sys.path.append("/usr/local/spark/spark-2.3.0-bin-hadoop2.7/python/lib")
import config
from datetime import datetime
import py4j
from pyspark.sql import SQLContext

#import parquet_file_creation
#import p3_final_beta
#import p10_final_beta


import schedule
import time

def job():
    t1 = datetime.now()
    #os.chdir(config.proj_path)

    print(os.getcwd())
    os.system("python "+config.proj_path+"/parquet_file_creation.py")
    os.system("python "+config.proj_path+"/source_code/p3_final_beta.py")
    os.system("python "+config.proj_path+"/source_code/p10_final_beta.py")
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

