#--------- p7 - bandwidth per location ---------------------------#

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
newpath = config.proj_path+'/log_for_demo/p7' 
if not os.path.exists(newpath):
    os.makedirs(newpath)

if(real_flag==1):   
    newpath = config.proj_path+'/p7' 
    if not os.path.exists(newpath):
        os.makedirs(newpath)

#for handler in logging.root.handlers[:]:
#    logging.root.removeHandler(handler)
    
logging.basicConfig(filename=config.proj_path+'/log_for_demo/p7/p7.log',level=logging.DEBUG)

def create_prophet_m(loc_name,z1,delay=24):
    
    ### --- For realtime pred ---###
    
    full_df = z1.bw.iloc[0:len(z1)]
    full_df = full_df.reset_index()
    full_df.columns = ['ds','y']
    
    #removing outliers
    q50 = full_df.y.median()
    q100 = full_df.y.quantile(1)
    q75  = full_df.y.quantile(.75)
    
    if((q100-q50) >= (2*q75)):
     
        full_df.loc[full_df.y>=(2*q75),'y'] = None
         
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
    #--- completes realtime pred ---#
    
    train_end_index=len(z1.bw)-delay
    train_df=z1.bw.iloc[0:train_end_index]
    
    
    test_df=z1.bw.iloc[train_end_index:len(z1)]
    
    
    
    train_df=train_df.reset_index()
    test_df=test_df.reset_index()
    train_df.columns=['ds','y']
    
    #--- removing outliers in trainset  ---#
    
    q50 = train_df.y.median()
    q100 = train_df.y.quantile(1)
    q75  = train_df.y.quantile(.75)
    
    if((q100-q50) >= (2*q75)):
        
        train_df.loc[train_df.y>=(2*q75),'y'] = None
    
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
        
        pred_df['error_test']=(pred_df.y-pred_df.yhat)
  
        MSE=mse(pred_df.y,pred_df.yhat)
        RMSE=math.sqrt(MSE)
        
        pred_df['APE']=abs(pred_df.error_test*100/pred_df.y)
        MAPE=pred_df.APE.mean()
        
        print("App name:",loc_name)
        
        print("RMSE :",RMSE)
        print("MAPE :",MAPE)
        
        mape_q98=pred_df['APE'][pred_df.APE<pred_df['APE'].quantile(0.98)].mean()

        df = pd.DataFrame({#'length':len(z1),
                             'test_rmse':RMSE,
                             'test_mape':MAPE,
                 
                 'test_mape_98':mape_q98},
                   
                          index=[loc_name])

    return(df,model,forecast,pred_df,pred_r)


def forcomb(l,temp,ftime1):
    
    temp2 = temp[temp.location==l]
    prophet_future_df = pd.DataFrame()
    prophet_analysis_df = pd.DataFrame()
    prophet_df = pd.DataFrame()
        
    if(len(temp2)>config.limit):
        
        prophet_analysis_df,p_model,p_forcast,prophet_df,prophet_future_df=(create_prophet_m(l,temp2,config.delay))
    
        prophet_future_df['location']=l
    
        prophet_analysis_df['location'] = l
        
        prophet_df['location'] = l
        t2 = datetime.now()
        prophet_analysis_df['total_run_time'] = round(((t2-ftime1).seconds/60),2)

        
    return  prophet_df, prophet_analysis_df, prophet_future_df

if __name__ == '__main__':
    # Reading data from parquet
    print('satrt quering')

    qt1 = datetime.now()

    df = sqlContext.read.parquet(config.proj_path+'/datas/appid_datapoint_parquet1')
    df2 =  sqlContext.read.parquet(config.proj_path+'/datas/appid_attribute_parquet')
    df2 = df2[['attribute_id','source','target_address','location']]

    #renaming the column
    from pyspark.sql.functions import col

    df2 = df2.select(col("attribute_id").alias("target_attribute_id"),
                       col("source").alias("source_y"),
                       col("target_address").alias("target_address_y"),
                       col("location").alias("location"), 
                      )
    # merging the dfs

    df_merge = df.join(df2,how='left',on='target_attribute_id')

    # Needed data extraction
    t1 = datetime.now()
    data = df_merge.registerTempTable('dummy')
    data = sqlContext.sql('select sum(byte_count) as byte_count_sum  , time_stamp, location from dummy group by location, time_stamp')
    data = data[data.byte_count_sum > 0]

    # data cleaning
    bw_df=data.toPandas()
    t2 =datetime.now()
    time_to_fetch = str(t2-t1)

    bw_df['bw'] = bw_df['byte_count_sum']/(8*3600)
    bw_df = bw_df.sort_values(by='location',ascending=True)       
    dates_outlook = pd.to_datetime(pd.Series(bw_df.time_stamp),unit='ms')
    bw_df.index = dates_outlook   
    bw_df = bw_df.sort_values(by='time_stamp')
    print('quering is successfull')



    logging.info(datetime.now())
    logging.info('-I- Fetching query successfull...')

    qt2 = datetime.now()
    query_time = str(qt2-qt1)

 
    # Running for all combiantions

    qt1 = datetime.now()

    l_list = bw_df.location.unique()
    prophet_df = pd.DataFrame()
    prophet_future_df = pd.DataFrame()
    prophet_analysis_df = pd.DataFrame()


    pool = Parallel(n_jobs=-1,verbose=5,pre_dispatch='all')
    r0  = pool(delayed(forcomb)(l,bw_df,qt1) for l in l_list) 


    for i in range(0,len(r0)):
        prophet_df = prophet_df.append(r0[i][0])
        prophet_analysis_df = prophet_analysis_df.append(r0[i][1])
        prophet_future_df = prophet_future_df.append(r0[i][2])
  
 
    print(' -I- dataframe cteated ')
    logging.info(datetime.now())
    logging.info('-I- Model ran succesdfully...')

    # saving as csv for graphical representation
    if(real_flag==1):
        prophet_analysis_df.to_csv(config.proj_path+'/p7/bw_analysis_per_location_data.csv',index=False)
        prophet_df.to_csv(config.proj_path+'/p7/bw_evaluation_per_location_data.csv',index=False)
        prophet_future_df.to_csv(config.proj_path+'/p7/bw_forecast_per_location_data.csv',index=False)
        bw_df.to_csv(config.proj_path+'/p7/bw_per_location_dataset.csv',index=False)

    qt2 = datetime.now()
    model_time  = str(qt2-qt1)



    # Writing the forecasted data to to mysql_db

    from sqlalchemy import create_engine

    engine = create_engine(str("mysql+pymysql://"+config.db_user+":"+config.db_pass+"@"+config.db_host+":"+str(config.db_port)+"/"+config.db_name))


    if(real_flag==1):
        prophet_future_df.to_sql(con=engine, name='forecast_bw_per_location', if_exists='replace',index=False)

    total_t2 = datetime.now()
    # calculating runtime in minuts
    total_real = (total_t2 - total_t1).seconds/60
    total_time = str(total_t2 - total_t1)

    #for analysis of our model in future

    prophet_analysis_df['run_date'] = datetime.now().date()
    #prophet_analysis_df['total_run_time'] = total_real
    prophet_analysis_df.index = list(range(0,len(prophet_analysis_df)))

    if(config.override_flag == 1):
        prophet_analysis_df.to_sql(con=engine, name='analyse_p7', if_exists='replace',index=False)
    else:
        prophet_analysis_df.to_sql(con=engine, name='analyse_p7', if_exists='append',index=False)

    print(total_time)
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