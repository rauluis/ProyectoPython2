import boto3
import pandas as pd
from io import StringIO, BytesIO #Se añadió BytesIO
from datetime import datetime, timedelta
import numpy as np
import matplotlib.pyplot as plt

arg_date = '2022-04-02'


arg_date_dt = datetime.strptime(arg_date, '%Y-%m-%d').date() - timedelta(days=1)

s3 = boto3.resource('s3')
bucket = s3.Bucket('deutsche-boerse-xetra-pds')

objects = [obj for obj in bucket.objects.all() if datetime.strptime(obj.key.split('/')[0], '%Y-%m-%d').date() >= arg_date_dt]

csv_obj_init = bucket.Object(key=objects[0].key).get().get('Body').read().decode('utf-8')
data = StringIO(csv_obj_init)
df_init = pd.read_csv(data, delimiter=',')

df_init.columns



df_all = pd.DataFrame(columns=df_init.columns)
for obj in objects:
    csv_obj = bucket.Object(key=obj.key).get().get('Body').read().decode('utf-8')
    data = StringIO(csv_obj)
    df = pd.read_csv(data, delimiter=',')
    df_all = df_all.append(df, ignore_index=True)


df_all['Datetime'] = pd.to_datetime(df_all['Date']+ ' ' +df_all['Time'])

df_all = df_all.infer_objects()


df_all.dtypes


columns = ['ISIN', 'Datetime', 'Date', 'Time', 'StartPrice', 'EndPrice']
df_all = df_all.loc[:, columns]

df_all['STD'] = df_all.std(axis= 1)



df_all['transformMNX'] = df_all['EndPrice'] * 22.99


df_all.plot(x='EndPrice', y='transformMNX', style='o')
plt.title('EndPrice vs transformMNX')
plt.xlabel('EndPrice') 
plt.ylabel('transformMNX') 
plt.show()



X = df_all['EndPrice'].values.reshape(-1,1)
y = df_all['transformMNX'].values.reshape(-1,1)



from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)




from sklearn.linear_model import LinearRegression
regressor = LinearRegression() 
regressor.fit(X_train, y_train) #Entrena el algoritmo 



#Para obtener el intercepto:
print(regressor.intercept_)
#Para obtener la pendiente
print(regressor.coef_)



y_pred = regressor.predict(X_test)



df_prediccion = pd.DataFrame({'Actual': y_test.flatten(), 'Predicted': y_pred.flatten()})


df_1=df_prediccion.head(100)
df_1.plot(kind='bar',figsize=(16,10))
plt.grid(which='major', linestyle='-', linewidth='0.5', color='green')
plt.grid(which='minor', linestyle=':', linewidth='0.5', color='black')
plt.show()




plt.scatter(X_test, y_test,  color='gray')
plt.plot(X_test, y_pred, color='red', linewidth=2)
plt.show()




df_all = pd.concat([df_all, df_prediccion])


df_all = df_all.infer_objects()



df_all.dtypes



df_all




df_all = df_all.set_index(['Datetime'])
df_all.between_time('08:00','12:00')



key = 'xetra_daily_report_' + datetime.today().strftime("%Y%m%d_%H%M%S") + '.parquet'



out_buffer = BytesIO()
df_all.to_parquet(out_buffer, index=True)
bucket_target = s3.Bucket('xetra-bucket-rizo1')
bucket_target.put_object(Body=out_buffer.getvalue(), Key=key)


for obj in bucket_target.objects.all():
    print(obj.key)


prq_obj = bucket_target.Object(key='xetra_daily_report_20220402_201012.parquet').get().get('Body').read()
data = BytesIO(prq_obj)
df_report = pd.read_parquet(data)


print(df_report)




