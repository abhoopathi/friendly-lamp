import pandas as pd
import config
import pymysql
from datetime import datetime

def connect_to_mysql():
    connection = pymysql.connect(host = config.db_host,
                            port= config.db_port,
                            user= config.db_user,
                            password= config.db_pass,
                            db= config.db_name,
                            charset='utf8',
                            cursorclass=pymysql.cursors.DictCursor)
    return connection

def retrieve(start_date , end_date):
    
    
    start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
    end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
    
    conn=connect_to_mysql()

    full_df = pd.DataFrame()

    for i in range(1,10):
        with conn.cursor() as cursor:
            # Read a  record
            sql = "select * from analyse_p"+str(i) 
            cursor.execute(sql)
            df = pd.DataFrame(cursor.fetchall())
    
        dates = df.run_date.unique()
        for j in dates:
        
            df1 = df[df.run_date==j]
        
            df1 = pd.DataFrame(df1.describe())
            df2 =  df1.reset_index()
            df2['report'] = "p"+str(i)
            df2['run_date'] = j
    
            full_df = full_df.append(df2)
    
    
    
    full_df1 = full_df[(full_df.run_date>=start_date) & (full_df.run_date<end_date)]
    #return full_df
    full_df1.index = list(range(0,len(full_df1)))
    full_df1 = full_df1.to_json()
    print(len(full_df1))
    return full_df1