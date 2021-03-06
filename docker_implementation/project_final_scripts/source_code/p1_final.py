#--------- p1 - app_rsp_time per target/source/application ---------------------------#

import pandas as pd
import pymysql
import warnings
warnings.filterwarnings("ignore")
from datetime import datetime, timedelta
import logging
from joblib import Parallel, delayed

from pyspark.sql import SQLContext

from tqdm import tqdm
from fbprophet import Prophet
from sklearn.metrics import mean_squared_error as mse
import math

import config
# flag to confirm the writting of forecasted value to db
real_flag = config.real_flag
total_t1 = datetime.now()
## Logging ##

import os
import sys


from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

# this is the configuration file

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

from pyspark.sql import SparkSession

full_t1 = datetime.now()
# initialise sparkContext
spark1 = SparkSession.builder \
    .master(config.sp_master) \
    .appName(config.sp_appname) \
    .config('spark.executor.memory', config.sp_memory) \
    .config("spark.cores.max", config.sp_cores) \
    .getOrCreate()

sc = spark1.sparkContext

# using SQLContext to read parquet file
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

## Logging ##
import os
import sys
newpath = config.proj_path+'/log_for_demo' 
if not os.path.exists(newpath):
    os.makedirs(newpath)
newpath = config.proj_path+'/log_for_demo/p1' 
if not os.path.exists(newpath):
    os.makedirs(newpath)

if(real_flag==1):    
    newpath =  config.proj_path+'/p1' 
    if not os.path.exists(newpath):
        os.makedirs(newpath)

#for handler in logging.root.handlers[:]:
#    logging.root.removeHandler(handler)
    
logging.basicConfig(filename= (config.proj_path+'/log_for_demo/p1/p1.log'),level=logging.DEBUG)


def create_prophet_m(app_name,z1,delay=24):
    
    ### --- For realtime pred ---###
    
    full_df = z1.app_rsp_time.iloc[0:len(z1)]
    full_df = full_df.reset_index()
    full_df.columns = ['ds','y']
    
    #removing outliers
    q50 = full_df.y.median()
    q100 = full_df.y.quantile(1)
    q75  = full_df.y.quantile(.75)
    
    if((q100-q50) >= (2*q50)):
        
        full_df.loc[full_df.y>=(2*q50),'y'] = None
    
    #-- Realtime prediction --##
    #model 
    model_r = Prophet(yearly_seasonality=False,changepoint_prior_scale=.2)
    model_r.fit(full_df)
    future_r = model_r.make_future_dataframe(periods=delay,freq='H')
    forecast_r = model_r.predict(future_r)
    forecast_r.index = forecast_r['ds']
    #forecast 
    pred_r = pd.DataFrame(forecast_r['yhat'][len(z1):(len(z1)+delay)])
    pred_r=pred_r.reset_index()
    #--- completes realtime prediction ---#
    
    train_end_index=len(z1.app_rsp_time)-delay
    train_df=z1.app_rsp_time.iloc[0:train_end_index]
    
    
    test_df=z1.app_rsp_time.iloc[train_end_index:len(z1)]
    
    
    
    train_df=train_df.reset_index()
    test_df=test_df.reset_index()
    train_df.columns=['ds','y']
    
    #--- removing outliers in trainset  ---#
    
    q50 = train_df.y.median()
    q100 = train_df.y.quantile(1)
    q75  = train_df.y.quantile(.75)
    
    if((q100-q50) >= (2*q50)):
        
        train_df.loc[train_df.y>=(2*q50),'y'] = None
    
    test_df.columns=['ds','y']
    
    #model 
    model = Prophet(yearly_seasonality=False,changepoint_prior_scale=.2)
    model.fit(train_df)
    future = model.make_future_dataframe(periods=len(test_df),freq='H')
    forecast = model.predict(future)
    forecast.index = forecast['ds']
    #forecast 
    pred = pd.DataFrame(forecast['yhat'][train_end_index:len(z1)])
    pred=pred.reset_index()
    pred_df=pd.merge(test_df,pred,on='ds',how='left')
    pred_df.dropna(inplace=True)
    
    df=pd.DataFrame()
    
    if(len(pred_df)>0):
        
        pred_df['error_test']=pred_df.y-pred_df.yhat
    
        
    
        MSE=mse(pred_df.y,pred_df.yhat)
        RMSE=math.sqrt(MSE)
        pred_df['APE']=abs(pred_df.error_test*100/pred_df.y)
        MAPE=pred_df.APE.mean()
        min_error_rate = pred_df.quantile(0)/100
        max_error_rate = pred_df.quantile(1)/100
        median_error_rate = pred_df.quantile(.50)/100
        print("App name:",app_name)
        print("MSE  :",MSE)
        print("RMSE :",RMSE)
        print("MAPE :",MAPE)
        
       
        mape_q98=pred_df['APE'][pred_df.APE<pred_df['APE'].quantile(0.98)].mean()
        std_MAPE = math.sqrt(((pred_df.APE-MAPE)**2).mean())

        df = pd.DataFrame({'length':len(z1),
                             'test_rmse':RMSE,
                             'test_mape':MAPE,
                             'std_mape':std_MAPE, #standerd deviation of mape
                             'min_error_rate':min_error_rate, 
                             'max_error_rate':max_error_rate ,
                             'median_error_rate':median_error_rate,
                 
                 'test_mape_98':mape_q98},
                   
                          index=[app_name])

    return(df,model,forecast,pred_df,pred_r)

#-- Function to select a combination for the run

def forapp(t,s,a,df,ftime1):
    
    
    df2 = df[(df.target_address == t)]

    
    df2 = df2[['app_rsp_time','application','source','target_address','time_stamp']]
    
    df2 = df2.sort_values(by='time_stamp')
    
    
    prophet_df = pd.DataFrame()
    prophet_analysis_df = pd.DataFrame()
    prophet_future_df = pd.DataFrame()
   
    
    
   
    if(len(df2)>config.limit):
            
        t2 = datetime.now()
            
        prophet_analysis_df,ew_model,ew_forcast,prophet_df,prophet_future_df =(create_prophet_m(a,df2,config.delay))
        t2 = datetime.now()
        
        prophet_analysis_df['application'] = a
        prophet_analysis_df['target'] = t
        prophet_analysis_df['source'] = s
        prophet_analysis_df['total_run_time'] = round(((t2-ftime1).seconds/60),2)
            
        prophet_future_df['application'] = a
        prophet_future_df['target'] = t
        prophet_future_df['source'] = s
        
        prophet_df['target'] = t
        prophet_df['source'] = s
        prophet_df['application'] = a
    
    return prophet_df, prophet_analysis_df, prophet_future_df,df2

if __name__ == '__main__':
    # Reading data from parquet
    print('satrt quering')

    qt1 = datetime.now()
    df = sqlContext.read.parquet(config.proj_path+'/datas/appid_datapoint_parquet1')
    df = df[df.app_rsp_time!=0]


    #--- Needed data extraction ---#

    # connecter to mysql db
    from sqlalchemy import create_engine
    engine = create_engine(str("mysql+pymysql://"+config.db_user+":"+config.db_pass+"@"+config.db_host+":"+str(config.db_port)+"/"+config.db_name))

    t1 = datetime.now()
    #result = pd.DataFrame()


    # Checking whether the reference data is available in db or not , if no then creating it
    with conn.cursor() as cursor:
            # Read a  record
            sql = "SHOW TABLES LIKE 'reference_df'" 
            cursor.execute(sql)
            result = (cursor.fetchall())

    if result:
    
        with conn.cursor() as cursor:
            # Read a  record
            sql = "select * from reference_df" 
            cursor.execute(sql)
            rdf = pd.DataFrame(cursor.fetchall())
        so_list = list(rdf.source)
        ap_list = list(rdf.application)
        ta_list = list(rdf.target_address)
        #data = df[(df.source == s ) & (df.application==a)]
    

    rdf1 = rdf[['application','source']]
    rdf1.drop_duplicates(inplace=True)
    ap_list = list(rdf1.application)
    so_list = list(rdf1.source)
    
    prophet_df = pd.DataFrame()
    prophet_future_df = pd.DataFrame()
    prophet_analysis_df = pd.DataFrame()
    app_rsp_time_full_df = pd.DataFrame() 

    qt2 = datetime.now()
    clean_time = str(qt2-qt1)
    logging.info('-I- full data cleaning time :')
    logging.info(clean_time)

    for k in tqdm(range(0,len(ap_list))):
        ftime1 = datetime.now()
        a = ap_list[k]
        s = so_list[k]
        ta1_list = list(rdf[(rdf.source==s) & (rdf.application==a)].target_address.unique())

        data = df[(df.source == s ) & (df.application==a)]
    
        data = data[data.target_address.isin(ta1_list)]

        # data cleaning
        app_rsp_time_df=data.toPandas()
        t2 = datetime.now()
        time_to_fetch = str(t2-t1)

        app_rsp_time_df = app_rsp_time_df.sort_values(by='app_rsp_time',ascending=True)       
        dates_outlook = pd.to_datetime(pd.Series(app_rsp_time_df.time_stamp),unit='ms')
        app_rsp_time_df.index = dates_outlook   
        app_rsp_time_df = app_rsp_time_df.sort_values(by='time_stamp')
        #app_rsp_time_df.to_csv('p1/app_rsp_time_per_source_per_app_per_target_dataset.csv',index=False)
        print('quering is successfull')



        logging.info(datetime.now())
        logging.info('-I- Fetching query successfull...')

        qt2 = datetime.now()
        query_time = str(qt2-qt1)

 
        # Running for all combiantions

        qt1 = datetime.now()

        pool = Parallel(n_jobs=-1,verbose=5,pre_dispatch='all')
        r0  = pool(delayed(forapp)(t,s,a,app_rsp_time_df,ftime1) for t in ta1_list) 

        #ftime2 = datetime.now()
    
        for i in range(0,len(r0)):
            prophet_df = prophet_df.append(r0[i][0])
            prophet_analysis_df = prophet_analysis_df.append(r0[i][1])
            prophet_future_df = prophet_future_df.append(r0[i][2])
            app_rsp_time_full_df = app_rsp_time_full_df.append(r0[i][3])
    
        qt2 = datetime.now()
        model_time  = str(qt2-qt1)

  
 
    print(' -I- dataframe cteated ')
    logging.info(datetime.now())
    logging.info('-I- Model ran succesdfully...')

    # saving as csv for graphical representation
    if(real_flag==1):
        app_rsp_time_full_df.to_csv( config.proj_path+'/p1/app_rsp_time_per_source_per_app_per_target_dataset.csv',index=False)
    
        prophet_analysis_df.to_csv( config.proj_path+'/p1/app_rsp_time_analysis_per_source_per_app_per_target_data.csv',index=False)
    
        prophet_df.to_csv( config.proj_path+'/p1/app_rsp_time_evaluation_per_source_per_app_per_target_data.csv',index=False)
        prophet_future_df.to_csv( config.proj_path+'/p1/app_rsp_time_forecast_per_source_per_app_per_target_data.csv',index=False)




    ##--- Writing the forecasted data to to mysql_db ---##
 
    # this flag decides whether the forcasted data write or not
    if(real_flag==1):
        prophet_future_df.to_sql(con=engine, name='forecast_p1', if_exists='replace', index=False )
        prophet_df.to_sql(con=engine, name='eval_p1', if_exists='replace', index=False )
        app_rsp_time_full_df.to_sql(con=engine, name='data_p1', if_exists='replace', index=False )

    conn.close()

    total_t2 = datetime.now()
    # calculating runtime in minuts
    total_real = (total_t2 - total_t1).seconds/60
    total_time = str(total_t2 - total_t1)
    #for analysis of our model in future

    prophet_analysis_df['run_date'] = datetime.now().date()
    #prophet_analysis_df['total_run_time'] = total_real
    prophet_analysis_df.index = list(range(0,len(prophet_analysis_df)))

    if(config.override_flag == 1):
        prophet_analysis_df.to_sql(con=engine, name='analyse_p1', if_exists='replace',index=False)
    else:
        prophet_analysis_df.to_sql(con=engine, name='analyse_p1', if_exists='append',index=False)
    
    run_time_table = pd.DataFrame({'report':'p1','run_time':total_real,'run_date':datetime.now().date()},index=[1])
    run_time_table.to_sql(con=engine, name='run_time_table', if_exists='append',index=False)
    ## Logging
    logging.info(datetime.now())
    logging.info('-I- validation of model...')
    logging.info(prophet_analysis_df)

    logging.info('-I- Run time for fetching the data from parquet file is')
    logging.info(query_time)
    logging.info('-I- Run time for modelling is ')
    logging.info(model_time)
    logging.info('-I- The total run time  is ')
    logging.info(total_time)
    print ('Total run time  is ', total_time)