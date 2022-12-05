# -*- coding: utf-8 -*-
"""
Created on 2022/9/23
@author: nhsiao

2022/9/5 avg_rsrp 改成 c_rsrp, 圖片從 2022/8/27閞始
2022/9/29 c_rsrp 改成 pos_first_rsrp, 圖片從 2022/9/23 閞始
"""

import cx_Oracle 
import pandas as pd
import gc
import gzip
# from datetime import datetime, timedelta
import func 
import numpy as np

import warnings
warnings.filterwarnings('ignore','.*Failed to load HostKeys.*')
warnings.filterwarnings('ignore')

localDir = 'D:\\Nicole\\python\\cottCNN\\data\\'

cott_duration = pd.date_range(start="2022-10-30",end="2022-10-31")

for f in cott_duration:
    
   
    df_site = pd.DataFrame(data=None, columns=['itt_id', 'site1','site2', 'site3', 'site1_dis','site2_dis', 'site3_dis'])
    df_site_new = pd.DataFrame(data=None, columns=['ittid', 'ittid_lat','ittid_long', 'site','site_dis', 'site_lat', 'site_long','site_type'])
    
    
    yesDay = f.strftime("%Y%m%d")
    yesDate = f.strftime("%Y-%m-%d")
    localDir = 'D:\\Nicole\\python\\cottCNN\\data\\'
    sFile = 'TT_Data_'+ yesDay +'.csv.gz'


    # 今日rawData
    with gzip.open(localDir + sFile, 'rb') as f:
            rawCott = pd.read_csv(f)
     
    sql = 'SELECT ITT_ID, to_char(CREATE_DATE,\'YYYY-MM-DD HH24\')||\':00\' event_date, to_char(CREATE_DATE-1,\'YYYY-MM-DD HH24\')||\':00\' event_date_24hr, to_char(CREATE_DATE-4,\'YYYY-MM-DD HH24\')||\':00\' event_start_date, GIS_X_84, GIS_Y_84 FROM ITSMRPT.RPT_COTT@ITSMRPT_NEW  WHERE trunc(CREATE_DATE) = TO_DATE(\''+ yesDate +'\',\'YYYY-MM-DD\') union SELECT ITT_ID, to_char(CREATE_DATE,\'YYYY-MM-DD HH24\')||\':00\' event_date, to_char(CREATE_DATE-1,\'YYYY-MM-DD HH24\')||\':00\' event_date_24hr, to_char(CREATE_DATE-4,\'YYYY-MM-DD HH24\')||\':00\' event_start_date, GIS_X_84, GIS_Y_84 FROM ITSMRPT.RPT_COTT_APP@ITSMRPT_NEW  WHERE trunc(CREATE_DATE) = TO_DATE(\''+ yesDate +'\',\'YYYY-MM-DD\')' 
    
    connection = cx_Oracle.connect('nocadm/noc2512@192.168.20.35/nois3g')
    df1 = pd.read_sql(sql, con=connection)
    
    pd.options.mode.chained_assignment = None  # default='warn'
    
    df3 = rawCott.merge(df1, left_on="itt_id", right_on="ITT_ID", how='left', suffixes=('_1', '_2'))
    df3['start_time'] = pd.to_datetime(df3['start_time'], format='%Y-%m-%d %H:%M:%S')
    
    condition = "`start_time` <= `EVENT_DATE` and start_time >= `EVENT_START_DATE`"
    df_raw0 = df3.query(condition, engine='python')
    
    df_raw = df_raw0[['itt_id','site_id', 'GIS_X_84', 'GIS_Y_84','c_lat','c_long', 'n_type', 'start_time','EVENT_START_DATE','EVENT_DATE', 'EVENT_DATE_24HR','duration','pos_first_rsrp', 'c_prbutil', 'c_rssi','end_cqi','call_type','dl_volume','dl_tput','pos_last_rsrq']]    
    
    df_raw["start_time"] = pd.to_datetime(df_raw["start_time"]) 
    df_raw['EVENT_START_DATE'] = pd.to_datetime(df_raw['EVENT_START_DATE'])
    df_raw['EVENT_DATE'] = pd.to_datetime(df_raw['EVENT_DATE'])
    df_raw['EVENT_DATE_24HR'] = pd.to_datetime(df_raw['EVENT_DATE_24HR'])

    df_raw['dl_volume'].fillna(value=0, inplace=True)
    df_raw['dl_volume'] = df_raw['dl_volume'].astype('int64')

    df_raw['dl_tput'].fillna(value=0, inplace=True)
    df_raw['dl_tput'] = df_raw['dl_tput'].astype('int64')

    df_raw['itt_id'] = df_raw['itt_id'].astype('str')

    df_raw['pos_first_rsrp_color'] = df_raw.apply(func.get_rsrp_color, axis=1).copy()
    df_raw['c_prbutil_color'] = df_raw.apply(func.get_prb_color, axis=1).copy()
    df_raw['c_rssi_color'] = df_raw.apply(func.get_rssi_color, axis=1).copy()
    df_raw['end_cqi_color'] = df_raw.apply(func.get_cqi_color, axis=1).copy()
    df_raw['dl_tput_color'] = df_raw.apply(func.get_dltput_color, axis=1).copy()
    df_raw['pos_last_rsrq_color'] = df_raw.apply(func.get_rsrq_color, axis=1).copy()
    
    df_raw['duration2'] = df_raw.apply(func.get_times, axis=1).copy()
    
    del rawCott
    del df3
    del df_raw0
    
    df_raw['itt_id'] = df_raw['itt_id'].astype('str')
    
    itt_id = df_raw['itt_id'].unique()
    
    # itt_id = ['M2210171089','M2210171088','M2210171083','M2210171082','M2210171080']
    # itt_id = ['M2210241220']
    
    for i in range(len(itt_id)):
        
        condition = "`itt_id` == '" + itt_id[i] + "'"
        df = df_raw.query(condition, engine='python')
        
        con = (df['site_id'].isin(['LS710360'])) & (df['start_time']> df['EVENT_DATE_24HR'])
        con_df = df[con]
                
        #取得停留最久的基站
        site1, tt1_lat, tt1_long, site1_lat, site1_long, bad_site1, bad_site1_lat, bad_site1_long, pos_first_rsrp_mean1,c_prbutil_mean1,c_rssi_mean1,dl_tput_mean1,pos_last_rsrq_mean1,end_cqi_mean1,pos_first_rsrp_count1,c_prbutil_count1,c_rssi_count1,dl_tput_count1,pos_last_rsrq_count1,end_cqi_count1, pos_first_rsrp_bmean1,c_prbutil_bmean1,c_rssi_bmean1,dl_tput_bmean1,pos_last_rsrq_bmean1,end_cqi_bmean1,pos_first_rsrp_bcount1,c_prbutil_bcount1,c_rssi_bcount1,dl_tput_bcount1,pos_last_rsrq_bcount1,end_cqi_bcount1 = func.get_site_id(df, 8, 12)
        site2, tt2_lat, tt2_long, site2_lat, site2_long, bad_site2, bad_site2_lat, bad_site2_long, pos_first_rsrp_mean2,c_prbutil_mean2,c_rssi_mean2,dl_tput_mean2,pos_last_rsrq_mean2,end_cqi_mean2,pos_first_rsrp_count2,c_prbutil_count2,c_rssi_count2,dl_tput_count2,pos_last_rsrq_count2,end_cqi_count2, pos_first_rsrp_bmean2,c_prbutil_bmean2,c_rssi_bmean2,dl_tput_bmean2,pos_last_rsrq_bmean2,end_cqi_bmean2,pos_first_rsrp_bcount2,c_prbutil_bcount2,c_rssi_bcount2,dl_tput_bcount2,pos_last_rsrq_bcount2,end_cqi_bcount2 = func.get_site_id(df, 12, 18)
        site3, tt3_lat, tt3_long, site3_lat, site3_long, bad_site3, bad_site3_lat, bad_site3_long, pos_first_rsrp_mean3,c_prbutil_mean3,c_rssi_mean3,dl_tput_mean3,pos_last_rsrq_mean3,end_cqi_mean3,pos_first_rsrp_count3,c_prbutil_count3,c_rssi_count3,dl_tput_count3,pos_last_rsrq_count3,end_cqi_count3, pos_first_rsrp_bmean3,c_prbutil_bmean3,c_rssi_bmean3,dl_tput_bmean3,pos_last_rsrq_bmean3,end_cqi_bmean3,pos_first_rsrp_bcount3,c_prbutil_bcount3,c_rssi_bcount3,dl_tput_bcount3,pos_last_rsrq_bcount3,end_cqi_bcount3 = func.get_site_id(df, 18, 24)
        
        site1_dis = ""
        site2_dis = ""
        site3_dis = ""
        bad_site1_dis = ""
        bad_site2_dis = ""
        bad_site3_dis = ""
        # if len(site1_lat) > 0:
        if site1_lat:
            # site1_dis = format(func.LLs2Dist(tt1_lat, tt1_long, site1_lat, site1_long),'.2f')
            site1_dis = func.round_v2(func.LLs2Dist(tt1_lat, tt1_long, site1_lat, site1_long),3)

        if site2_lat:                
            site2_dis = func.round_v2(func.LLs2Dist(tt2_lat, tt2_long, site2_lat, site2_long),3)
        if site3_lat:    
            site3_dis = func.round_v2(func.LLs2Dist(tt3_lat, tt3_long, site3_lat, site3_long),3)
            
        if bad_site1_lat:
            bad_site1_dis = func.round_v2(func.LLs2Dist(tt1_lat, tt1_long, bad_site1_lat, bad_site1_long),3)
        if bad_site2_lat:                
            bad_site2_dis = func.round_v2(func.LLs2Dist(tt2_lat, tt2_long, bad_site2_lat, bad_site2_long),3)
        if bad_site3_lat:    
            bad_site3_dis = func.round_v2(func.LLs2Dist(tt3_lat, tt3_long, bad_site3_lat, bad_site3_long),3)

        df_site = df_site.append({'itt_id' :itt_id[i] , 'site1' : site1, 'site2' : site2, 'site3' : site3
                                  , 'site1_dis' : site1_dis 
                                  , 'site2_dis' : site2_dis 
                                  , 'site3_dis' : site3_dis 
                                  } , ignore_index=True)
        
        site_arr = [site1, site2, site3, bad_site1, bad_site2, bad_site3]
        ittid_lat_arr =  [tt1_lat, tt2_lat, tt3_lat, tt1_lat, tt2_lat, tt3_lat]
        ittid_long_arr =  [tt1_long, tt2_long, tt3_long, tt1_long, tt2_long, tt3_long]
        site_dis_arr =  [site1_dis, site2_dis, site3_dis, bad_site1_dis, bad_site2_dis, bad_site3_dis]
        site_lat_arr =  [site1_lat, site2_lat, site3_lat, bad_site1_lat, bad_site2_lat, bad_site3_lat]
        site_long_arr = [site1_long, site2_long, site3_long, bad_site1_long, bad_site2_long, bad_site3_long]
        site_type_arr = ['time1', 'time2', 'time3', 'btime1', 'btime2', 'btime3']


        #6-1參數
        rsrp_mean_arr = [pos_first_rsrp_mean1, pos_first_rsrp_mean2, pos_first_rsrp_mean3, pos_first_rsrp_bmean1, pos_first_rsrp_bmean2, pos_first_rsrp_bmean3]
        rsrp_count_arr = [pos_first_rsrp_count1, pos_first_rsrp_count2, pos_first_rsrp_count3,pos_first_rsrp_bcount1, pos_first_rsrp_bcount2, pos_first_rsrp_bcount3]

        # rsrp_bmean_arr = [pos_first_rsrp_bmean1, pos_first_rsrp_bmean2, pos_first_rsrp_bmean3]
        # rsrp_bcount_arr = [pos_first_rsrp_bcount1, pos_first_rsrp_bcount2, pos_first_rsrp_bcount3]
        
        #6-2參數
        prbutil_mean_arr = [c_prbutil_mean1, c_prbutil_mean2, c_prbutil_mean3, c_prbutil_bmean1, c_prbutil_bmean2, c_prbutil_bmean3]
        prbutil_count_arr = [c_prbutil_count1, c_prbutil_count2, c_prbutil_count3, c_prbutil_bcount1, c_prbutil_bcount2, c_prbutil_bcount3]
        
        # prbutil_bmean_arr = [c_prbutil_bmean1, c_prbutil_bmean2, c_prbutil_bmean3]
        # prbutil_bcount_arr = [c_prbutil_bcount1, c_prbutil_bcount2, c_prbutil_bcount3]
        
        #6-3參數
        rssi_mean_arr = [c_rssi_mean1, c_rssi_mean2, c_rssi_mean3, c_rssi_bmean1, c_rssi_bmean2, c_rssi_bmean3]
        rssi_count_arr = [c_rssi_count1, c_rssi_count2, c_rssi_count3, c_rssi_bcount1, c_rssi_bcount2, c_rssi_bcount3]

        # rssi_bmean_arr = [c_rssi_bmean1, c_rssi_bmean2, c_rssi_bmean3]
        # rssi_bcount_arr = [c_rssi_bcount1, c_rssi_bcount2, c_rssi_bcount3]


        #6-4參數
        dltput_mean_arr = [dl_tput_mean1, dl_tput_mean2, dl_tput_mean3, dl_tput_bmean1, dl_tput_bmean2, dl_tput_bmean3]
        dltput_count_arr = [dl_tput_count1, dl_tput_count2, dl_tput_count3, dl_tput_bcount1, dl_tput_bcount2, dl_tput_bcount3]                
        
        # dltput_bmean_arr = [dl_tput_bmean1, dl_tput_bmean2, dl_tput_bmean3]
        # dltput_bcount_arr = [dl_tput_bcount1, dl_tput_bcount2, dl_tput_bcount3]                
        
        #6-5參數
        rsrq_mean_arr = [pos_last_rsrq_mean1, pos_last_rsrq_mean2, pos_last_rsrq_mean3, pos_last_rsrq_bmean1, pos_last_rsrq_bmean2, pos_last_rsrq_bmean3]
        rsrq_count_arr = [pos_last_rsrq_count1, pos_last_rsrq_count2, pos_last_rsrq_count3, pos_last_rsrq_bcount1, pos_last_rsrq_bcount2, pos_last_rsrq_bcount3]    

        # rsrq_bmean_arr = [pos_last_rsrq_bmean1, pos_last_rsrq_bmean2, pos_last_rsrq_bmean3]
        # rsrq_bcount_arr = [pos_last_rsrq_bcount1, pos_last_rsrq_bcount2, pos_last_rsrq_bcount3]               
        
        #6-6參數
        cqi_mean_arr = [end_cqi_mean1, end_cqi_mean2, end_cqi_mean3, end_cqi_bmean1, end_cqi_bmean2, end_cqi_bmean3]
        cqi_count_arr = [end_cqi_count1, end_cqi_count2, end_cqi_count3, end_cqi_bcount1, end_cqi_bcount2, end_cqi_bcount3] 
        
        # cqi_bmean_arr = [end_cqi_bmean1, end_cqi_bmean2, end_cqi_bmean3]
        # cqi_bcount_arr = [end_cqi_bcount1, end_cqi_bcount2, end_cqi_bcount3] 
        
        for a in range(len(site_arr)):
            df_site_new = df_site_new.append({'ittid' :itt_id[i] 
                                              , 'ittid_lat' : ittid_lat_arr[a]
                                              , 'ittid_long' : ittid_long_arr[a]
                                              , 'site' : site_arr[a]
                                              , 'site_dis' : site_dis_arr[a] 
                                              , 'site_lat' : site_lat_arr[a] 
                                              , 'site_long' : site_long_arr[a] 
                                              , 'site_type' : site_type_arr[a]
                                              , 'pos_first_rsrp_mean' : rsrp_mean_arr[a]
                                              , 'pos_first_rsrp_count' : rsrp_count_arr[a]
                                              , 'c_prbutil_mean' : prbutil_mean_arr[a]
                                              , 'c_prbutil_count' : prbutil_count_arr[a]
                                              , 'c_rssi_mean' : rssi_mean_arr[a]
                                              , 'c_rssi_count' : rssi_count_arr[a]
                                              , 'dl_tput_mean' : dltput_mean_arr[a]
                                              , 'dl_tput_count' : dltput_count_arr[a]
                                              , 'pos_last_rsrq_mean' : rsrq_mean_arr[a]
                                              , 'pos_last_rsrq_count' : rsrq_count_arr[a]
                                              , 'end_cqi_mean' : cqi_mean_arr[a]
                                              , 'end_cqi_count' : cqi_count_arr[a]
                                              } , ignore_index=True)
        
    # 倒入ORACLE
    df_site_new = df_site_new[df_site_new['site'].notna()]

    for i, j in df_site_new.iterrows():
        # print(j['ittid'], j['ittid_lat'], j['ittid_long'], j['site'], j['site_dis'], j['site_lat'], j['site_long'], j['site_type'], j['pos_first_rsrp_mean'],  j['pos_first_rsrp_count'], j['c_prbutil_mean'], j['c_prbutil_count'], j['c_rssi_mean'], j['c_rssi_count'],  j['dl_tput_mean'], j['dl_tput_count'], j['pos_last_rsrq_mean'],  j['pos_last_rsrq_count'],  j['end_cqi_mean'], j['end_cqi_count'])
        
        func.insert_orcl(j['ittid'], j['ittid_lat'], j['ittid_long'], j['site'], j['site_dis'], j['site_lat'], j['site_long'], j['site_type'], j['pos_first_rsrp_mean'],  j['pos_first_rsrp_count'], j['c_prbutil_mean'], j['c_prbutil_count'], j['c_rssi_mean'], j['c_rssi_count'],  j['dl_tput_mean'], j['dl_tput_count'], j['pos_last_rsrq_mean'],  j['pos_last_rsrq_count'],  j['end_cqi_mean'], j['end_cqi_count']) 
            
        # print(f)
        # print(df.shape[0])
        
    # del df_raw0

    
    # print ("\ngarbage len", len(gc.garbage))
    # print ("garbages:", gc.garbage)
    gc.collect()
    
    # keep record time
    df_site.to_csv('D:\\Nicole\\python\\cottCNN\\sitelist_with_dis.csv', mode='a',index=False)
    df_site_new.to_csv('D:\\Nicole\\python\\cottCNN\\df_site_new_with_dis.csv', mode='a',index=False)
    
    # del df_raw
    # del df
    # del df_site
    # del df_site_new

