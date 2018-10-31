import os
import sys

import config
from datetime import datetime
import pymysql
import pandas as pd

## connection to partitiion table to get required apps
def connect_to_mysql2():
    connection = pymysql.connect(host = config.db_host,
                            port= config.db_port,
                            user= config.db_user,
                            password= config.db_pass,
                            db= config.db_name2,
                            charset='utf8',
                            cursorclass=pymysql.cursors.DictCursor)
    return connection

import schedule
import time

def job():
    t1 = datetime.now()
    #os.chdir(config.proj_path)

    print(os.getcwd())
    #conn2=connect_to_mysql2()

    
    os.system("python "+config.proj_path+"/parquet_file_creation_agg.py")
    os.system("python "+config.proj_path+"/p3_final_agg.py")
    os.system("python "+config.proj_path+"/p10_final_agg.py")

    t2 = datetime.now()

    print('Total time to run all scripts : ',str(t2-t1))

## scheduling the task every night 12am
#schedule.every().day.at("00:00").do(job)

job()

#while True:
#    schedule.run_pending()
#    time.sleep(60) # wait one minute


