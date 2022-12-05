# -*- coding: utf-8 -*-
"""
Created on Wed Oct 12 15:42:48 2022
@author: noc
"""

import math
import numpy as np
import config as cfg
import pysftp
import pandas as pd



def round_v2(num, decimal):
    num = np.round(num, decimal)
    num = float(num)
    return num

def LLs2Dist(lat1, lon1, lat2, lon2):
    R = 6371
    dLat = (lat2 - lat1) * math.pi / 180.0
    dLon = (lon2 - lon1) * math.pi / 180.0
    a = math.sin(dLat / 2) * math.sin(dLat / 2) + math.cos(lat1 * math.pi / 180.0) * math.cos(lat2 * math.pi / 180.0) * math.sin(dLon / 2) * math.sin(dLon / 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    dist = R * c
    return dist


#Red的次數加倍統計,而非多筆只算1次,造成誤判
#2022/11/25,加入權重計算
def get_duration(df):
    color_arr = ['pos_first_rsrp_color','c_prbutil_color','c_rssi_color','dl_tput_color','pos_last_rsrq_color','end_cqi_color']
    times_arr = [10,10,6,6,3,3]

    times = 0
    for c in range(len(color_arr)): 
        if df[color_arr[c]] == 'red':
            #times = times +1
            times = times + times_arr[c]
            
    if times == 0:
        times =1
    
    if isinstance(df['duration'], type(pd.NaT)):
        df['duration'] = 0
 
    return str(round(df['duration'] * times))


#2022/11/30, 計算次數, 加入權重
def get_times(df):
    color_arr = ['pos_first_rsrp_color','c_prbutil_color','c_rssi_color','dl_tput_color','pos_last_rsrq_color','end_cqi_color']
    times_arr = [10,10,6,6,3,3]

    times = 1
    for c in range(len(color_arr)): 
        if df[color_arr[c]] == 'red':
            times = times + times_arr[c]
    return times


def get_site_id(df, time1, time2):
   
    # 24小時內, 體驗不佳, 且為工單和User在兩公里內的站台
    bad_condtion = ((df['start_time'].dt.hour.isin(np.arange(time1,time2))) & (df['start_time']> df['EVENT_DATE_24HR']) &  ((df['pos_first_rsrp_color']=='red') | (df['dl_tput_color']=='red') |   (df['c_rssi_color']=='red') |  (df['end_cqi_color']=='red') |  (df['pos_last_rsrq_color']=='red')  |  (df['c_rssi_color']=='red') | (df['c_prbutil_color']=='red') ))
            
   
    bad_df_time = df[bad_condtion]
    
    try: 
        bad_site_df = bad_df_time.groupby(by='site_id', as_index=False)['times'].sum().sort_values('times', ascending=False).head(1)
        bad_site = bad_site_df['site_id'].values
        
        bad_latlon = bad_df_time[bad_df_time['site_id'].isin(bad_site)].head(1)
        bad_site_lat = round_v2(bad_latlon['c_lat'].values[0],3)
        bad_site_long = round_v2(bad_latlon['c_long'].values[0],4)
        
    except: 
        bad_site = ['']
        bad_site_lat = ""
        bad_site_long = ""
        
    
    # 取得24小時內平均值,總數(體驗不佳)
    mean_arr = ['pos_first_rsrp','c_prbutil','c_rssi','dl_tput','pos_last_rsrq','end_cqi']
    for a in range(len(mean_arr)): 
        
        #該站,該時段
        bad_con_df = ""
        bad_con =  (bad_df_time['site_id'].isin(bad_site)) & (bad_df_time[mean_arr[a]+'_color'].str.contains("red"))  
        bad_con_df = bad_df_time[bad_con]
        
        try: 
            globals()[mean_arr[a]+'_bmean'] = round_v2(bad_con_df[mean_arr[a]].mean(),3)
            globals()[mean_arr[a]+'_bcount'] = round_v2(bad_con_df[mean_arr[a]].count(),3)
            
            if isinstance(globals()[mean_arr[a]+'_bmean'], float) and math.isnan(globals()[mean_arr[a]+'_bmean']):
                globals()[mean_arr[a]+'_bmean'] = ''
            elif isinstance(globals()[mean_arr[a]+'_bmean'], type(pd.NaT)):
                globals()[mean_arr[a]+'_bmean'] = ''

            if isinstance(globals()[mean_arr[a]+'_bcount'], float) and math.isnan(globals()[mean_arr[a]+'_bmean']):
                globals()[mean_arr[a]+'_bcount'] = ''
            elif isinstance(globals()[mean_arr[a]+'_bcount'], type(pd.NaT)):
                globals()[mean_arr[a]+'_bcount'] = ''
                
            
        except :
            globals()[mean_arr[a]+'_bmean'] = ""
            globals()[mean_arr[a]+'_bcount'] = ""
            
            
    
    # 4天內, 使用最久的Site與經緯度
    condtion = df['start_time'].dt.hour.isin(np.arange(time1,time2)) 
    df_time = df[condtion]
    
    #Red的次數加倍統計,而非多筆只算1次,造成誤判
    # df_time['duration2'] = df_time.apply(get_times, axis=1).copy()
    
    
    
    try:
        site = df_time.groupby(by='site_id', as_index=False)['times'].sum().sort_values('times', ascending=False).head(1)['site_id'].values
        
        latlon = df_time[df_time['site_id'].isin(site)].head(1)
        
        tt_y = latlon['GIS_Y_84'].values.astype(float)[0]
        tt_x = latlon['GIS_X_84'].values.astype(float)[0]
        site_lat = latlon['c_lat'].values[0]
        site_long = latlon['c_long'].values[0]
        
        
    except: 
        site = ['']
        tt_y = ""
        tt_x = ""
        site_lat = ""
        site_long = ""
        


    #取得24小時內之平均值與總數, 此值借存放在time1中(time1: 4天)
    # con = (df_time['site_id'].isin(site)) & (df_time['start_time']> df_time['EVENT_DATE_24HR'])
    # 取消此設定: (df_time['site_id'].isin(site)) 
    con = (df['start_time'].dt.hour.isin(np.arange(time1,time2))) & (df['start_time']> df['EVENT_DATE_24HR'])
    con_df = df[con]
    
    mean_arr = ['pos_first_rsrp','c_prbutil','c_rssi','dl_tput','pos_last_rsrq','end_cqi']
    for a in range(len(mean_arr)): 
        
        con_df_var = ""
        con_df_var = con_df[con_df[mean_arr[a]+'_color'] != 'white']
        try :
            
            globals()[mean_arr[a]+'_mean'] = round_v2(con_df_var[mean_arr[a]].mean(),3)
            globals()[mean_arr[a]+'_count'] = round_v2(con_df_var[mean_arr[a]].count(),3)
            
            if isinstance(globals()[mean_arr[a]+'_mean'], float) and math.isnan(globals()[mean_arr[a]+'_mean']):
                globals()[mean_arr[a]+'_mean'] = ''
            elif isinstance(globals()[mean_arr[a]+'_mean'], type(pd.NaT)):
                globals()[mean_arr[a]+'_mean'] = ''

            if isinstance(globals()[mean_arr[a]+'_count'], float) and math.isnan(globals()[mean_arr[a]+'_count']):
                globals()[mean_arr[a]+'_count'] = ''
            elif isinstance(globals()[mean_arr[a]+'_count'], type(pd.NaT)):
                globals()[mean_arr[a]+'_count'] = ''
                
            
        except :
            globals()[mean_arr[a]+'_mean'] = ""
            globals()[mean_arr[a]+'_count'] = ""


    return site[0], tt_y, tt_x, site_lat , site_long, bad_site[0], bad_site_lat, bad_site_long, pos_first_rsrp_mean,c_prbutil_mean,c_rssi_mean,dl_tput_mean,pos_last_rsrq_mean,end_cqi_mean,pos_first_rsrp_count,c_prbutil_count,c_rssi_count,dl_tput_count,pos_last_rsrq_count,end_cqi_count, pos_first_rsrp_bmean,c_prbutil_bmean,c_rssi_bmean,dl_tput_bmean,pos_last_rsrq_bmean,end_cqi_bmean,pos_first_rsrp_bcount,c_prbutil_bcount,c_rssi_bcount,dl_tput_bcount,pos_last_rsrq_bcount,end_cqi_bcount




def sftp(sFile, localDir):
    sHostName = cfg.imgserver
    sUserName = cfg.imguser
    sPassWord = cfg.imgpass
    
    cnopts = pysftp.CnOpts(knownhosts='known_hosts')
    cnopts.hostkeys = None
     
    with pysftp.Connection(sHostName, username=sUserName, password=sPassWord, cnopts=cnopts) as sftp:

        # 移動目錄
        sftp.cwd('./others/')
        # 檔案下載 sftp.get('遠端檔案位置', '本機檔案位置')
        sftp.get(sFile , localDir + sFile)


def get_rsrq_color(df):
    if  df['pos_last_rsrq'] < -16 and  df['duration'] < 60 :
        return 'white'
    elif df['pos_last_rsrq'] < -16 :
        return 'red'
    else:
        return 'green'

def get_dltput_color(df):
    if  df['dl_tput'] <= 10  and  df['duration'] < 60 :
        return 'white'
    elif df['dl_tput'] == 0 :
        return 'white'
    elif df['dl_tput'] <= 10 :
        return 'red'
    else:
        return 'green'

def get_rsrp_color(df):
    if df['pos_first_rsrp'] < -105 and  df['duration'] < 60  : #原為-105=>-115, 改回-105
        return 'white'
    elif df['pos_first_rsrp'] < -105 : #原為-105=>-115, 改回-105
        return 'red'    
    # ps掉3G
    elif df['n_type'] =='3G' and (df['call_type'] =='12' or df['call_type'] =='13' or df['call_type'] =='21')  and  df['duration'] < 60 :
        return 'white'
    elif df['n_type'] =='3G' and (df['call_type'] =='12' or df['call_type'] =='13' or df['call_type'] =='21')   :
        return 'red'
    elif pd.isna(df['pos_first_rsrp']) :
        return 'white'
    elif df['n_type'] == '4G' :
        return 'green'
    elif df['n_type'] == '3G' or df['n_type'] == '5G':
        return 'green'
    else:
        return 'white'
    
def get_prb_color(df):
    if df['c_prbutil'] > 0.85 and  df['duration'] < 60 :  #原為0.85=>0.9=>0.85
        return 'white'
    elif df['c_prbutil'] > 0.85  :  #原為0.85=>0.9=>0.85
        return 'red'
    # ps掉3G
    elif df['n_type'] =='3G' and (df['call_type'] =='12' or df['call_type'] =='13' or df['call_type'] =='21') and  df['duration'] < 60 :
        return 'white'
    elif df['n_type'] =='3G' and (df['call_type'] =='12' or df['call_type'] =='13' or df['call_type'] =='21')  :
        return 'red'
    elif pd.isna(df['c_prbutil']) :
        return 'white'
    else:
        return 'green'
    
def get_rssi_color(df):
    if df['c_rssi'] > -95 and  df['duration'] < 60:  #原為>-105
        return 'white'
    elif df['c_rssi'] > -95 :  #原為>-105
        return 'red'
    # ps掉3G
    elif df['n_type'] =='3G' and (df['call_type'] =='12' or df['call_type'] =='13' or 
                                  df['call_type'] =='21') and  df['duration'] < 60:
        return 'white'
    elif df['n_type'] =='3G' and (df['call_type'] =='12' or df['call_type'] =='13' 
                                  or df['call_type'] =='21') :
        return 'red'
    elif pd.isna(df['c_rssi']):
        return 'white'
    else:
        return 'green'
    
def get_cqi_color(df):
    if df['n_type'] == '4G' and df['end_cqi'] <= 7 and df['end_cqi'] >0   and  df['duration'] < 60:  #cqi=0,unkown, 
        return 'white'
    elif df['n_type'] == '4G' and df['end_cqi'] <= 7 and df['end_cqi'] >0  :  #cqi=0,unkown, 
        return 'red'
    elif  df['n_type'] == '4G'  and df['end_cqi'] >7 :  #
        return 'green'
    else:
        return 'white'

import cx_Oracle
def insert_orcl_del(cott, predict_type, site1, site2, site3, site1_dis, site2_dis, site3_dis):
    sql = ('insert into nocadm.predict_cott(cott, predict_type, site1, site2, site3, site1_dis, site2_dis, site3_dis) '
        'values(:cott,:predict_type, :site1, :site2, :site3, :site1_dis, :site2_dis, :site3_dis) LOG ERRORS INTO nocadm.error_log_nois REJECT LIMIT UNLIMITED')
    try:
        # establish a new connection
        with cx_Oracle.connect(cfg.username,
                            cfg.password,
                            cfg.dsn,
                            encoding=cfg.encoding) as connection:
            # create a cursor
            with connection.cursor() as cursor:
                # execute the insert statement
                cursor.execute(sql, [cott, predict_type, site1, site2, site3, site1_dis, site2_dis, site3_dis])
                # commit work
                connection.commit()
    except cx_Oracle.Error as error:
        print('Error occurred:')
        print(error)



def insert_orcl(itt_id,ittid_lat,ittid_long,site ,site_dis,site_lat ,site_long,site_type,pos_first_rsrp_mean,	pos_first_rsrp_count,		c_prbutil_mean,	c_prbutil_count,	c_rssi_mean,	c_rssi_count,	dl_tput_mean,	dl_tput_count,	pos_last_rsrq_mean,	pos_last_rsrq_count,	end_cqi_mean,end_cqi_count):
    sql = ('insert into nocadm.predict_cott_sites(itt_id,ittid_lat,ittid_long,site ,site_dis,site_lat ,site_long,site_type, pos_first_rsrp_mean,  pos_first_rsrp_count,  c_prbutil_mean, c_prbutil_count,c_rssi_mean, c_rssi_count,  dl_tput_mean,  dl_tput_count,  pos_last_rsrq_mean, pos_last_rsrq_count, end_cqi_mean, end_cqi_count) '
        'values(:itt_id,:ittid_lat,:ittid_long, :site , :site_dis, :site_lat , :site_long, :site_type, :pos_first_rsrp_mean,  :pos_first_rsrp_count,  :c_prbutil_mean, :c_prbutil_count,:c_rssi_mean, :c_rssi_count,  :dl_tput_mean,  :dl_tput_count,  :pos_last_rsrq_mean, :pos_last_rsrq_count,  :end_cqi_mean, :end_cqi_count ) LOG ERRORS INTO nocadm.error_log_nois REJECT LIMIT UNLIMITED')
    
    # print(sql)
    
    try:
        # establish a new connection
        with cx_Oracle.connect(cfg.username,
                            cfg.password,
                            cfg.dsn,
                            encoding=cfg.encoding) as connection:
            # create a cursor
            with connection.cursor() as cursor:
                # execute the insert statement
                cursor.execute(sql, [itt_id,ittid_lat,ittid_long,site ,site_dis,site_lat ,site_long,site_type,pos_first_rsrp_mean,pos_first_rsrp_count,c_prbutil_mean,c_prbutil_count,c_rssi_mean,c_rssi_count,dl_tput_mean,dl_tput_count,pos_last_rsrq_mean,pos_last_rsrq_count,end_cqi_mean,end_cqi_count])
                # commit work
                connection.commit()
    except cx_Oracle.Error as error:
        print('Error occurred:')
        print(error)


def insert_orcl_ori(cott, predict_type, site1, site2, site3, site1_dis, site2_dis, site3_dis):
    sql = ('insert into nocadm.predict_cott(cott, predict_type, site1, site2, site3, site1_dis, site2_dis, site3_dis) '
        'values(:cott,:predict_type, :site1, :site2, :site3, :site1_dis, :site2_dis, :site3_dis) LOG ERRORS INTO nocadm.error_log_nois REJECT LIMIT UNLIMITED')
    try:
        # establish a new connection
        with cx_Oracle.connect(cfg.username,
                            cfg.password,
                            cfg.dsn,
                            encoding=cfg.encoding) as connection:
            # create a cursor
            with connection.cursor() as cursor:
                # execute the insert statement
                cursor.execute(sql, [cott, predict_type, site1, site2, site3, site1_dis, site2_dis, site3_dis])
                # commit work
                connection.commit()
    except cx_Oracle.Error as error:
        print('Error occurred:')
        print(error)

