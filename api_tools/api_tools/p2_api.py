class p2:

    def __init__(self):

        from pyspark.sql import SQLContext
        import pandas as pd
        import pymysql
        import warnings
        warnings.filterwarnings("ignore")
        from datetime import datetime, timedelta
        import logging
        from tqdm import tqdm
        from fbprophet import Prophet
        from sklearn.metrics import mean_squared_error as mse
        import math

        import config

        ## Logging ##

        import os
        import sys


        from pyspark.context import SparkContext
        from pyspark.sql.session import SparkSession
        import os
        import pandas as pd

        from pyspark.sql import SparkSession
        import datetime
        full_t1 = datetime.datetime.now()


        
    
        import pymysql


        def connect_to_mysql(self):
            connection = pymysql.connect(host = config.db_host,
                                port= config.db_port,
                                user= config.db_user,
                                password= config.db_pass,
                                db= config.db_name,
                                charset='utf8',
                                cursorclass=pymysql.cursors.DictCursor)
            return connection

        self.conn=connect_to_mysql(self)
        self.p2_df = pd.DataFrame()
        

        

        import warnings
        warnings.filterwarnings('ignore')
        
        import numpy as np


    ## Modelling

    def create_prophet_m(self,source_name,z1,delay=24):
    
        import pandas as pd
        import pymysql
        import warnings
        warnings.filterwarnings("ignore")
        from datetime import datetime, timedelta
        import logging
        from tqdm import tqdm
        from fbprophet import Prophet
        from sklearn.metrics import mean_squared_error as mse
        import math

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
            #print("App name:",source_name)
            ##print("MSE  :",MSE)
            #print("RMSE :",RMSE)
            #print("MAPE :",MAPE)

            q98=pred_df['APE'].quantile(0.98)
            mape_q98=pred_df['APE'][pred_df.APE<pred_df['APE'].quantile(0.98)].mean()

            df = pd.DataFrame({#'length':len(z1),
                                 'test_rmse':RMSE,
                                 'test_mape':MAPE,

                     'test_mape_98':mape_q98},
                              index=[source_name])

        return(df,model,forecast,pred_df,pred_r)
 

    ##Function to load or refresh data
    def get_latest_data(self):
        from pyspark.sql import SparkSession
        import config
        import pandas as pd
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

        from datetime import datetime
        t1 = datetime.now()
        df = sqlContext.read.parquet(config.proj_path+'/datas/appid_datapoint_parquet1')
        # creating and querying fron the temporory table
        df1 = df.registerTempTable('dummy')
        df1 = sqlContext.sql('select count(distinct application) as app_count, time_stamp, source from dummy group by source, time_stamp')

        # data cleaning
        self.p2_df = df1.toPandas()
        
        dates_outlook = pd.to_datetime(pd.Series(self.p2_df.time_stamp),unit='ms')
        self.p2_df.index = dates_outlook   
        self.p2_df['date'] = self.p2_df.index.date
        self.p2_df = self.p2_df.sort_values(by='time_stamp')
    
        t2 =datetime.now()
        time_to_fetch = str(t2-t1)

        #return self.p2_df

    ## Main function for the demo graph
    def forcomb(self,s):

        import pandas as  pd
        import config
        from datetime import datetime

        #self.get_latest_data()
        
    
        temp2 = self.p2_df[self.p2_df.source==s]
        prophet_future_df = pd.DataFrame()
        prophet_analysis_df = pd.DataFrame()
        prophet_df = pd.DataFrame()

        if(len(temp2)>config.limit):

            prophet_analysis_df,p_model,p_forcast,prophet_df,prophet_future_df=(self.create_prophet_m(s,temp2,config.delay))

            t2 = datetime.now()

            prophet_future_df['source']=s

            prophet_analysis_df['source'] = s
            #prophet_analysis_df['total_run_time'] = round(((t2-ftime1).seconds/60),2)

            prophet_df['source'] = s


        return  prophet_df, prophet_analysis_df, prophet_future_df,temp2
        


