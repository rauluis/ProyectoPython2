from sqlite3 import adapt
import boto3
import pandas as pd
from io import StringIO, BytesIO
from datetime import datetime, timedelta


#class ClaseLoad():


#class ClaseTransform():
   
#class ClaseXtract():
    

class ClaseadapterLayer():
    
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
        return bucket_target
        
    def return_objects(bucket,key,src_format,arg_date):
        arg_date_dt = datetime.strptime(arg_date, src_format).date() - timedelta(days=1)
        objects = [obj for obj in bucket.objects.all() if datetime.strptime(obj.key.split('/')[0], src_format).date() >= arg_date_dt]
        return objects

class ClaseapplicationLayer():

    def extract(key,objects,bucket):
        df_all = pd.concat([ClaseadapterLayer.read_csv_to_df(bucket,obj.key) for obj in objects], ignore_index=True)
        
        return df_all

    def transform_report(df_all,arg_date,columns):
        
        df_all = df_all.loc[:, columns]
        df_all.dropna(inplace=True)
        
        #get opening price per ISIN and Day
        df_all['opening_price'] = df_all.sort_values(by=['Time']).groupby(['ISIN', 'Date'])['StartPrice'].transform('first')
        
        #Get closing price per ISIN and Day
        df_all['closing_price'] = df_all.sort_values(by=['Time']).groupby(['ISIN', 'Date'])['StartPrice'].transform('last')
        
        #Agregation
        df_all = df_all.groupby(['ISIN', 'Date'], as_index=False).agg(opening_price_eur=('opening_price', 'min'), closing_price_eur=('closing_price', 'min'), minimum_price_eur=('MinPrice', 'min'), maximum_price_eur=('MaxPrice', 'max'), daily_traded_volume=('TradedVolume', 'sum'))
    
        #Percent Change Prev Closing
        df_all['prev_closing_price'] = df_all.sort_values(by=['Date']).groupby(['ISIN'])['closing_price_eur'].shift(1)
        df_all['change_prev_closing_%'] = (df_all['closing_price_eur'] - df_all['prev_closing_price']) / df_all['prev_closing_price'] * 100
        
        df_all.drop(columns=['prev_closing_price'], inplace=True)
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
        

    def etl_report(key,columns,objects,bucket,arg_date,s3,trg_bucket):
        #extraer ,transormar, cargar/load
        
        df_all = ClaseapplicationLayer.extract(key,objects,bucket)

        df_all = ClaseapplicationLayer.transform_report(df_all,arg_date,columns)
    
        data = ClaseapplicationLayer.load(s3,trg_bucket,df_all,key)
        df_report = pd.read_parquet(data)
        print("Esto es el df report",df_report)
        return df_report


class main():
    arg_date = ''
    src_format = '%Y-%m-%d'
    src_bucket = 'deutsche-boerse-xetra-pds'
    trg_bucket = 'xetra-bucket-rizo1'
    columns = ['ISIN', 'Date', 'Time', 'StartPrice', 'MaxPrice', 'MinPrice', 'EndPrice', 'TradedVolume']
    key = 'xetra_daily_report_' + datetime.today().strftime("%Y%m%d_%H%M%S") + '.parquet'
    
    # Init
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(src_bucket)
    
    # run application
    arg_date= input("Ingresa una fecha yyyy-mm-dd:")  
    objects= ClaseadapterLayer.return_objects(bucket,key,src_format,arg_date)

    ClaseapplicationLayer.etl_report(key,columns,objects,bucket,arg_date,s3,trg_bucket)
    


