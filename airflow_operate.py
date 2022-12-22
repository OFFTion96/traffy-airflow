#
from airflow.utils.dates import days_ago
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
import json
import requests
import pandas as pd
import numpy as np
from datetime import datetime
import os
from google.cloud import bigquery


def retrive_data():
    t_start = datetime.now()
    response = requests.get("https://publicapi.traffy.in.th/share/teamchadchart/search").json()
    n_total = response['total']
    n_loop = int(n_total/1000)
    n_loop_fail = []
    for i_loop in range(n_loop):

        try:
            offset = i_loop*1000
            response_data = requests.get("https://publicapi.traffy.in.th/share/teamchadchart/search?offset={}".format(offset)).json()
            if response_data['total']==None:
                break
            else:
                print("Running at loop {}".format(i_loop))
                data_result = response_data['results']
                df = pd.DataFrame.from_dict(data_result)
                df = df.drop(columns=['problem_type_abdul','status','star','count_reopen'])
                cols_name = ['message_id','type','type_id','org','comment','ticket_id','coords','photo','after_photo','address','district','subdistrict','province','timestamp','state']
                for i in cols_name:
                    df[i] = df[i].apply(lambda x: np.NaN if x =='' else x)
                def change_format_time(x):
                    time_obj = datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f+00")
                    return time_obj.strftime("%Y-%m-%d %H:%M:%S")
                df['timestamp'] = df['timestamp'].apply(change_format_time)
                df['timestamp'] = pd.to_datetime(df['timestamp'],format="%Y-%m-%d %H:%M:%S",errors='ignore', utc=True)
                def split_lat(x):
                    return x[1]
                def split_long(x):
                    return x[0]
                df['lat'] = df['coords'].apply(split_lat)
                df['long'] = df['coords'].apply(split_long)

                df = df.drop(columns=['coords'])
                
                df.to_csv('/home/apisakch11/airflow/data_tmp/data_from_api_{}.csv'.format(offset),index=False)
        except:
            n_loop_fail.append(i_loop)
    return offset

def append_data():
    df=pd.DataFrame()
    path = "/home/apisakch11/airflow/data_tmp"
    files = os.listdir(path)
    files = [os.path.join(path,f) for f in files]
    for i in files:
        data_read = pd.read_csv(i,index_col=False,dtype={'message_id':'string','type_id':'string','lat':'string','long':'string'})
        df = df.append(data_read)
    df = df.reset_index(drop=True)
    df = df.dropna(subset=['ticket_id'])
    df = df.drop_duplicates(subset=['ticket_id'])
    dt_now = datetime.now()
    df['data_updated'] = dt_now.strftime('%Y-%m-%d %H:%M:%S')
    df['data_updated'] = pd.to_datetime(df['data_updated'],format="%Y-%m-%d %H:%M:%S",errors='ignore', utc=True)
    time_file = dt_now.strftime('%Y-%m-%d_%H:%M:%S')
    df.to_csv('gs://off_dataeng_bucket/traffy_data_cleaned/traffy_data_batch_{}.csv'.format(time_file),index=False)
    return time_file

def merge_data(time_file):
    df = pd.read_csv('gs://off_dataeng_bucket/traffy_data_cleaned/traffy_data_batch_{}.csv'.format(time_file), index_col=False)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=""
    client = bigquery.Client()
    query_job = client.query(
        """
        SELECT *
        FROM `my-gcp-369016.traffy_fondue_project.traffy_data`
        """
    )

    df_bq = query_job.to_dataframe()

    id_df_bq = df_bq['ticket_id'].to_list()
    df_not_in_bq = (df[~df['ticket_id'].isin(id_df_bq)])
    id_df = df['ticket_id'].to_list()
    df_bq_not_in_df = df_bq[~df_bq['ticket_id'].isin(id_df)]
    df_merge = df_bq.merge(df,on='ticket_id',how='inner')

    def state_change1(state_x,state_y,data_updated_x,data_updated_y,after_photo_x,after_photo_y):
        if state_x != state_y:
            data_updated = data_updated_y
            state=state_y
            after_photo = after_photo_y
        else:
            data_updated = data_updated_x
            state=state_x
            after_photo = after_photo_x
        return data_updated
    
    df_merge['data_updated']  = df_merge.apply(lambda x:  state_change1(x['state_x'],x['state_y'],x['data_updated_x'],x['data_updated_y'],x['after_photo_x'],x['after_photo_y']),axis=1)

    def state_change2(state_x,state_y,data_updated_x,data_updated_y,after_photo_x,after_photo_y):
        if state_x != state_y:
            data_updated = data_updated_y
            state=state_y
            after_photo = after_photo_y
        else:
            data_updated = data_updated_x
            state=state_x
            after_photo = after_photo_x
        return after_photo
    
    df_merge['after_photo']  = df_merge.apply(lambda x:  state_change2(x['state_x'],x['state_y'],x['data_updated_x'],x['data_updated_y'],x['after_photo_x'],x['after_photo_y']),axis=1)

    def state_change3(state_x,state_y,data_updated_x,data_updated_y,after_photo_x,after_photo_y):
        if state_x != state_y:
            data_updated = data_updated_y
            state=state_y
            after_photo = after_photo_y
        else:
            data_updated = data_updated_x
            state=state_x
            after_photo = after_photo_x
        return state
    
    df_merge['state']  = df_merge.apply(lambda x:  state_change3(x['state_x'],x['state_y'],x['data_updated_x'],x['data_updated_y'],x['after_photo_x'],x['after_photo_y']),axis=1)

    columns_drop = ['state_x','after_photo_x','data_updated_x','message_id_y','type_y', 'type_id_y',
       'org_y', 'comment_y', 'photo_y', 'after_photo_y', 'address_y',
       'district_y', 'subdistrict_y', 'province_y', 'timestamp_y', 'state_y',
       'lat_y', 'long_y', 'data_updated_y']
    df_merge = df_merge.drop(columns=columns_drop)

    rename_columns_dict = {'message_id_x':'message_id','type_id_x':'type_id','type_x':'type','org_x':'org','comment_x' :'comment','photo_x':'photo',
                       'after_photo_x':'after_photo','address_x':'address','district_x':'district','subdistrict_x':'subdistrict',
                       'province_x':'province','timestamp_x':'timestamp','state_x':'state','lat_x':'lat','long_x':'long',                      
                }
    df_merge = df_merge.rename(columns=rename_columns_dict) 

    cols = df_merge.columns.to_list()
    cols = cols[:7]+cols[-2:-1]+cols[7:12]+cols[-1:]+cols[12:-2]
    df_merge = df_merge[cols]
    df_concat = pd.concat([df_merge,df_not_in_bq,df_bq_not_in_df], ignore_index=True)
    df_concat = df_concat.drop_duplicates(subset=['ticket_id'])
    
    df_concat.to_csv('gs://off_dataeng_bucket/traffy_data_to_bq/traffy_data_to_bq.csv',index=False)
    df_concat.to_csv('gs://off_dataeng_bucket/traffy_data_to_bq_backup/traffy_data_to_bq_{}.csv'.format(time_file),index=False)





# 'with' enables DAG to become context managers; automatically assign new operators to that DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 11, 26),
    'schedule_interval': '0 */2 * * *',
}

dag = DAG('get_data_to_bq', catchup=False, default_args=default_args,schedule_interval='0 */2 * * *')


start = DummyOperator(task_id='start_task',dag=dag)

operate_retrive_data = PythonOperator(task_id='operate_retrive_data', python_callable=retrive_data,dag=dag)

append_data = PythonOperator(task_id='append_data', python_callable=append_data,dag=dag)

merge_data = PythonOperator(task_id='merge_data', python_callable=merge_data,op_args=[append_data.output],dag=dag)

mv_file_to_bucket = BashOperator(task_id='mv_file_to_bucket', bash_command='gsutil mv ~/airflow/data_tmp/data_from_api.csv gs://off_dataeng_bucket' )
load_data_to_bq = BashOperator(task_id='load_data_to_bq', bash_command='bq load --source_format=CSV --allow_quoted_newlines  \
        --autodetect --replace my-gcp-369016:traffy_fondue_project.traffy_data gs://off_dataeng_bucket/traffy_data_to_bq/traffy_data_to_bq.csv',dag=dag) 


rm_file_after = BashOperator(task_id='rm_file_after', bash_command='rm -f /home/apisakch11/airflow/data_tmp/*.csv',dag=dag )
rm_file_before = BashOperator(task_id='rm_file_before', bash_command='rm -f /home/apisakch11/airflow/data_tmp/*.csv',dag=dag )

# creating DAG dependencies can be a long flow or multiple short flows
start >> rm_file_before >> operate_retrive_data >> append_data >> merge_data >> load_data_to_bq >> rm_file_after
