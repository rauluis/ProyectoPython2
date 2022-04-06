from mimetypes import init
from sqlite3 import adapt
import boto3
import pandas as pd
from io import StringIO, BytesIO
from datetime import datetime, timedelta

import matplotlib.pyplot as plt
import numpy as np
from sklearn import datasets, linear_model
from sklearn.metrics import mean_squared_error, r2_score

class ClaseXtract():
    def __init__(self):
        self.arg_date=input("Ingresa una fecha yyyy-mm-dd:")
        self.src_format = '%Y-%m-%d'
        self.key = 'xetra_daily_report_' + datetime.today().strftime("%Y%m%d_%H%M%S") + '.parquet'
        self.src_bucket = 'deutsche-boerse-xetra-pds'
          
class ClaseTransform():
    def __init__(self,df_all,arg_date):
        self.df_all=df_all
        self.arg_date= arg_date
        self.columns =  ['ISIN', 'Date', 'Time', 'StartPrice', 'EndPrice']
       
class ClaseLoad():
    def __init__(self,df_all):   
        self.trg_bucket = 'xetra-bucket-rizo1'
        self.df_all= df_all
        self.key = 'xetra_daily_report_' + datetime.today().strftime("%Y%m%d_%H%M%S") + '.parquet'

class ClaseadapterLayer():

    def return_objects(bucket,key,src_format,arg_date):
        arg_date_dt = datetime.strptime(arg_date, src_format).date() - timedelta(days=1)
        objects = [obj for obj in bucket.objects.all() if datetime.strptime(obj.key.split('/')[0], src_format).date() >= arg_date_dt]
        return objects

    def read_csv_to_df(bucket,key):
        csv_obj = bucket.Object(key=key).get().get('Body').read().decode('utf-8')
        data = StringIO(csv_obj)
        df = pd.read_csv(data, delimiter=',')
        return df
        
    def write_df_to_s3(s3,trg_bucket,df_all,key):
        out_buffer = BytesIO()
        df_all.to_parquet(out_buffer, index=False)
        bucket_target = s3.Bucket(trg_bucket)
        bucket_target.put_object(Body=out_buffer.getvalue(), Key=key)
        return True
        
class ClaseapplicationLayer():

    def extract(bucket,key,objects):
               
        df_all = pd.concat([ClaseadapterLayer.read_csv_to_df(bucket,obj.key) for obj in objects], ignore_index=True)
        
        return df_all

    def transform_report(df_all,arg_date,columns):
        
        df_all = df_all.loc[:, columns]
        df_all.dropna(inplace=True)

        df_all= df_all.loc[(df_all["Time"] >= '08:00') & (df_all["Time"]<='12:00') , ["ISIN", "Date","Time","StartPrice","EndPrice"]]
        df_all=df_all[df_all['ISIN']=='AT0000A0E9W5']

        df_all['std']=df_all[["StartPrice", "EndPrice"]].std(axis=1)
        df_all["EndPrice_MXN"]= df_all["EndPrice"] * 22.94
        
        df_all = df_all.round(decimals=2)
        df_all = df_all[df_all.Date >= arg_date]
        
        print("Esto es el df_all dentro de Transform", df_all)
        return df_all

    def load(s3,trg_bucket,df_all,key):
        bucket_target = ClaseadapterLayer.write_df_to_s3(s3,trg_bucket,df_all,key)
        
        listObj_key= []     
        for obj in bucket_target.objects.all():
            print(obj.key)
            listObj_key.append(obj.key)
            
        last_Key = listObj_key[-1]
        
        prq_obj = bucket_target.Object(key=last_Key).get().get('Body').read()
        data = BytesIO(prq_obj) 
    
        return data
        
class ClaseEtl():
    def etl_report():
        #extraer ,transormar, cargar/load
         

        objExtract = ClaseXtract()
        
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(objExtract.src_bucket) 

        objects=ClaseadapterLayer.return_objects(bucket,objExtract.key,objExtract.src_format,objExtract.arg_date)

        df_all = ClaseapplicationLayer.extract(bucket,objExtract.key,objects)

        objTransform = ClaseTransform(df_all,objExtract.arg_date)

        df_all = ClaseapplicationLayer.transform_report(objTransform.df_all,objTransform.arg_date,objTransform.columns)
        
        # regresion.cosas(df_all)
        objLoad = ClaseLoad(df_all)
        data = ClaseapplicationLayer.load(s3,objLoad.trg_bucket,df_all,objLoad.key)

        df_report = pd.read_parquet(data)

        print("Esto es el df report",df_report)
        return df_report

class regresion():
    def cosas(df_all):
        X = df_all['EndPrice']
        y = df_all['Time'].replace({':':'.'}, regex=True).astype(float)
        #y=y.head(n=50000)
        #X=X.head(n=50000)
        plt.xlim(0, 300)
        plt.ylim(7.5, 12)
        plt.plot(X,y, color="red")
        plt.grid(alpha=0.3)
        y=y.reset_index().values
        X=X.reset_index().values 
        X_train = X[:-70]
        X_test = X[-70:]
        y_train =y[:-70]
        y_test = y[-70:]
        regr = linear_model.LinearRegression().fit(X_train,y_train)
        y_pred = regr.predict(X_test)
        print("Coeficientes: \n", regr.coef_)
        print("Error cuadratico medio: %.2f" % mean_squared_error(y_test, y_pred))
        # El coeficiente de determinacion: 1 de prediccion en perfecto
        print("Coeficinete de determinacion: %.2f" % r2_score(y_test,y_pred))
        plt.scatter(X_test, y_test, color="blue")
        plt.plot(X_test,y_pred, color="red")

        plt.grid(alpha=0.3)
        plt.show()
        return True

class main():
    ClaseEtl.etl_report()
    


