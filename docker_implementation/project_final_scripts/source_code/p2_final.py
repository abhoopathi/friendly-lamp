#--------- p2 - Number of application per source ---------------------------#

import pandas as pd
import config
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

# flag to confirm the writting of forecasted value to db
real_flag = config.real_flag
total_t1 = datetime.now()
## Logging ##

import os
import sys


from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

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

newpath = config.proj_path+'/log_for_demo' 
if not os.path.exists(newpath):
    os.makedirs(newpath)
newpath = config.proj_path+'/log_for_demo/p2' 
if not os.path.exists(newpath):
    os.makedirs(newpath)

if(real_flag==1):    
    newpath = config.proj_path+'/p2' 
    if not os.path.exists(newpath):
        os.makedirs(newpath)


#for handler in logging.root.handlers[:]:
#    logging.root.removeHandler(handler)
    
logging.basicConfig(filename=config.proj_path+'/log_for_demo/p2/p2.log',level=logging.DEBUG)


def create_prophet_m(source_name,z1,delay=24):
    
   
    train_end_index=len(z1.app_count)-delay
    train_df=z1.app_count.iloc[0:train_end_index]
    #train_df= train_df[train_df<cutter]
    full_df = z1.app_count.iloc[0:len(z1)]
    
    
    test_df=z1.app_count.iloc[train_end_index:len(z1)]
    
    
    
    train_df=train_df.reset_index()
    test_df=test_df.reset_index()
    train_df.columns=['ds','y']
    
    full_df = full_df.reset_index()
    full_df.columns = ['ds','y']
    
    test_df.columns=['ds','y']
    
    ##-- Realtime prediction --##
    #model 
    model_r = Prophet(yearly_seasonality=False,changepoint_prior_scale=.2)
    model_r.fit(full_df)
    future_r = model_r.make_future_dataframe(periods=delay,freq='H')
    forecast_r = model_r.predict(future_r)
    forecast_r.index = forecast_r['ds']
    #forecast 
    pred_r = pd.DataFrame(forecast_r['yhat'][len(z1):(len(z1)+delay)])
    pred_r=pred_r.reset_index()
    
    
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
        print("App name:",source_name)
        print("MSE  :",MSE)
        print("RMSE :",RMSE)
        print("MAPE :",MAPE)
        
        q98=pred_df['APE'].quantile(0.98)
        mape_q98=pred_df['APE'][pred_df.APE<pred_df['APE'].quantile(0.98)].mean()
        std_MAPE = math.sqrt(((pred_df.APE-MAPE)**2).mean())

        df = pd.DataFrame({'length':len(z1),
                             'test_rmse':RMSE,
                             'test_mape':MAPE,
                             'std_mape':std_MAPE, #standerd deviation of mape
                             'min_error_rate':min_error_rate ,
                             'max_error_rate':max_error_rate ,
                             'median_error_rate':median_error_rate,
                 
                 'test_mape_98':mape_q98},
                          index=[source_name])

    return(df,model,forecast,pred_df,pred_r)


def forsource(s,temp,ftime1):
    
    temp2 = temp[temp.source==s]
    prophet_future_df = pd.DataFrame()
    prophet_analysis_df = pd.DataFrame()
    prophet_df = pd.DataFrame()
        
    if(len(temp2)>config.limit):
        
        prophet_analysis_df,p_model,p_forcast,prophet_df,prophet_future_df=(create_prophet_m(s,temp2,config.delay))
        
        t2 = datetime.now()
        
        prophet_future_df['source']=s
    
        prophet_analysis_df['source'] = s
        prophet_analysis_df['total_run_time'] = round(((t2-ftime1).seconds/60),2)
        
        prophet_df['source'] = s
    
    
    return  prophet_df, prophet_analysis_df, prophet_future_df

if __name__ == '__main__':
    # Reading data from parquet
    print('satrt quering')

    qt1 = datetime.now()


    df = sqlContext.read.parquet(config.proj_path+'/datas/appid_datapoint_parquet1')
    # creating and querying fron the temporory table
    df1 = df.registerTempTable('dummy')
    df1 = sqlContext.sql('select count(distinct application) as app_count, time_stamp, source from dummy group by source, time_stamp')

    # data cleaning
    app_count_df=df1.toPandas()
        
    dates_outlook = pd.to_datetime(pd.Series(app_count_df.time_stamp),unit='ms')
    app_count_df.index = dates_outlook   
    app_count_df = app_count_df.sort_values(by='time_stamp')

    print('quering is successfull')



    logging.info(datetime.now())
    logging.info('-I- Fetching query successfull...')

    qt2 = datetime.now()
    query_time = str(qt2-qt1)

 
    # Running for all combiantions
    
    qt1 = datetime.now()

    s_list = app_count_df.source.unique()
    prophet_df = pd.DataFrame()
    prophet_future_df = pd.DataFrame()
    prophet_analysis_df = pd.DataFrame()

    pool = Parallel(n_jobs=-1,verbose=5,pre_dispatch='all')
    r0  = pool(delayed(forsource)(s,app_count_df,qt1) for s in s_list) 


    for i in range(0,len(r0)):
        prophet_df = prophet_df.append(r0[i][0])
        prophet_analysis_df = prophet_analysis_df.append(r0[i][1])
        prophet_future_df = prophet_future_df.append(r0[i][2])
 
 
    print(' -I- dataframe cteated ')
    logging.info(datetime.now())
    logging.info('-I- Model ran succesdfully...')

    # saving as csv for graphical representation
    if(real_flag==1):
        prophet_analysis_df.to_csv(config.proj_path+'/p2/application_count_analysis_data.csv',index=False)
        prophet_df.to_csv(config.proj_path+'/p2/application_count_evaluation_data.csv',index=False)
        prophet_future_df.to_csv(config.proj_path+'/p2/application_count_future.csv',index=False)
        app_count_df.to_csv(config.proj_path+'/p2/app_count_dataset.csv',index=False)

    qt2 = datetime.now()
    model_time  = str(qt2-qt1)



    # Writing the forecasted data to to mysql_db

    from sqlalchemy import create_engine

    engine = create_engine(str("mysql+pymysql://"+config.db_user+":"+config.db_pass+"@"+config.db_host+":"+str(config.db_port)+"/"+config.db_name))
#res22.to_sql(con=engine, name='dummy', if_exists='replace')
  
    if(real_flag==1):
        prophet_future_df.to_sql(con=engine, name='forecast_p2', if_exists='replace', index=False )
        prophet_df.to_sql(con=engine, name='eval_p2', if_exists='replace', index=False )
        app_count_df.to_sql(con=engine, name='data_p2', if_exists='replace', index=False )


    total_t2 = datetime.now()
    # calculating runtime in minuts
    total_real = (total_t2 - total_t1).seconds/60
    total_time = str(total_t2 - total_t1)

    #for analysis of our model in future

    prophet_analysis_df['run_date'] = datetime.now().date()
    #prophet_analysis_df['total_run_time'] = total_real
    prophet_analysis_df.index = list(range(0,len(prophet_analysis_df)))
    
    if(config.override_flag == 1):
        prophet_analysis_df.to_sql(con=engine, name='analyse_p2', if_exists='replace',index=False)
    else:
        prophet_analysis_df.to_sql(con=engine, name='analyse_p2', if_exists='append',index=False)

    print(total_time)
    run_time_table = pd.DataFrame({'report':'p2','run_time':total_real,'run_date':datetime.now().date()},index=[2])
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