# -*- coding: utf-8 -*-
"""
Created on 2022/9/23
@author: nhsiao

2022/9/5 avg_rsrp 改成 c_rsrp, 圖片從 2022/8/27閞始
2022/10/4  c_rsrp 改成 pos_first_rsrp
"""

import cx_Oracle 
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import dates as mpl_dates
import gc
import pysftp
import gzip
from datetime import datetime, timedelta
import config as cfg

import warnings
warnings.filterwarnings('ignore','.*Failed to load HostKeys.*')
warnings.filterwarnings('ignore')
# import datetime
# today = datetime.date.today().strftime("%Y-%m-%d")

# # keep process time
# now = datetime.now()
# txt = 'generateImg.py, 上次更新時間,From：' + str(now)
# df = pd.DataFrame([txt], index=['UpdateTime'])
# df.to_csv('D:\\Nicole\\python\\cottCNN\\logCottCNN.csv', mode='a',header=False)

#------------------------------------------------------
today = datetime.today().strftime("%Y-%m-%d")
yesterday = datetime.today() - timedelta(days=1)
yesDay = yesterday.strftime('%Y%m%d')
yesDate = yesterday.strftime('%Y-%m-%d')

today = "2022-10-08"
yesDay = "20221008"
yesDate = "2022-10-08"
#------------------------------------------------------

localDir = 'D:\\Nicole\\python\\cottCNN\\data\\'
sFile = 'TT_Data_'+ yesDay +'.csv.gz'

# 今日rawData
with gzip.open(localDir + sFile, 'rb') as f:
        rawCott = pd.read_csv(f)
    
ttdata = rawCott[rawCott.itt_id=='M2210080032']
ttdata.to_csv('D:\\Nicole\\python\\cottCNN\\M2210080032.csv')


ttdata = ttdata[['itt_id', 'n_type', 'start_time','duration','pos_first_rsrp', 'c_prbutil', 'c_rssi','end_cqi','call_type','dl_volume','dl_tput','pos_last_rsrq']]  

rawCott = ttdata


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

sftp(sFile, localDir)


 
sql = 'SELECT ITT_ID, to_char(CREATE_DATE,\'YYYY-MM-DD HH24\')||\':00\' event_date, to_char(CREATE_DATE-4,\'YYYY-MM-DD HH24\')||\':00\' event_start_date FROM ITSMRPT.RPT_COTT@ITSMRPT_NEW  WHERE trunc(CREATE_DATE) = TO_DATE(\''+ yesDate +'\',\'YYYY-MM-DD\') union SELECT ITT_ID, to_char(CREATE_DATE,\'YYYY-MM-DD HH24\')||\':00\' event_date, to_char(CREATE_DATE-4,\'YYYY-MM-DD HH24\')||\':00\' event_start_date FROM ITSMRPT.RPT_COTT_APP@ITSMRPT_NEW  WHERE trunc(CREATE_DATE) = TO_DATE(\''+ yesDate +'\',\'YYYY-MM-DD\')' 

connection = cx_Oracle.connect('nocadm/noc2512@192.168.20.35/nois3g')
df1 = pd.read_sql(sql, con=connection)

pd.options.mode.chained_assignment = None  # default='warn'

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

df3 = rawCott.merge(df1, left_on="itt_id", right_on="ITT_ID", how='left', suffixes=('_1', '_2'))
df3['start_time'] = pd.to_datetime(df3['start_time'], format='%Y-%m-%d %H:%M:%S')

condition = "`start_time` <= `EVENT_DATE` and start_time >= `EVENT_START_DATE`"
df_raw0 = df3.query(condition, engine='python')

df_raw = df_raw0[['itt_id', 'n_type', 'start_time','EVENT_START_DATE','EVENT_DATE','duration','pos_first_rsrp', 'c_prbutil', 'c_rssi','end_cqi','call_type','dl_volume','dl_tput','pos_last_rsrq']]    

df_raw["start_time"] = pd.to_datetime(df_raw["start_time"]) 
df_raw['EVENT_START_DATE'] = pd.to_datetime(df_raw['EVENT_START_DATE'])
df_raw['EVENT_DATE'] = pd.to_datetime(df_raw['EVENT_DATE'])

# del df2
del rawCott
del df3
del df_raw0

params = ["pos_first_rsrp", "c_prbutil", "c_rssi","end_cqi","pos_last_rsrq", "dl_tput"]

df_raw['dl_volume'].fillna(value=0, inplace=True)
df_raw['dl_volume'] = df_raw['dl_volume'].astype('int64')

df_raw['dl_tput'].fillna(value=0, inplace=True)
df_raw['dl_tput'] = df_raw['dl_tput'].astype('int64')

df_raw['itt_id'] = df_raw['itt_id'].astype('str')

df_raw['pos_first_rsrp_color'] = df_raw.apply(get_rsrp_color, axis=1).copy()
df_raw['c_prbutil_color'] = df_raw.apply(get_prb_color, axis=1).copy()
df_raw['c_rssi_color'] = df_raw.apply(get_rssi_color, axis=1).copy()
df_raw['end_cqi_color'] = df_raw.apply(get_cqi_color, axis=1).copy()

df_raw['dl_tput_color'] = df_raw.apply(get_dltput_color, axis=1).copy()
df_raw['pos_last_rsrq_color'] = df_raw.apply(get_rsrq_color, axis=1).copy()

itt_id = df_raw['itt_id'].unique()
for i in range(len(itt_id)):
    
    condition = "`itt_id` == '" + itt_id[i] + "'"
    df = df_raw.query(condition, engine='python')
    
    print(f)
    print(df.shape[0])
    
    #碓認資料完整性
    x0 = df.shape[0]
    x1 = df.c_prbutil.dropna().shape[0]
    x2 = df.pos_first_rsrp.dropna().shape[0]
    x3 = df.c_rssi.dropna().shape[0]
    
    if x1 <= 20 and x2 <= 20 and x3 <= 20 :
        continue
    
    plt.close('all')
    fig = plt.figure()
    plt.clf()
    
    fig, ax = plt.subplots(len(params), 1, sharex=True, figsize=(10, 13))
            
    for t in range(len(params)):
        print(t)
        print(params[t])

        condition = "`itt_id` == '" + itt_id[i] + "' and " + params[t] + "_color !='white'"
        df = df_raw.query(condition, engine='python').reset_index()
        # print(f)
        # print(df.shape[0])
    
        try :
            
            if params[t] == 'dl_volume' or params[t] == 'dl_tput':
                ax[t].bar(x=df['start_time'], height=df[params[t]].astype(int),
                            bottom=0,color=df[params[t] + '_color'], width =0.05, alpha=0.5)  
                #, edgecolor='grey'
                plt.ylim(0, 20)   
                ax[t].set_ylabel(params[t].upper(), fontsize=14)
                #matplotlib.pyplot.ylim(top=top_value)
                
            else:
                ax[t].scatter(x=df['start_time'], 
                        y=df[params[t]], 
                        s=df['duration'],
                        alpha=0.5, 
                        c=df[params[t] + '_color'],
                        cmap='viridis', ) 
                if params[t] == 'end_cqi' :
                    plt.ylim(0, 15) 
            # ax[t].set_ylabel(params[t].upper().split("_", 1)[1], fontsize=14)
            ax[t].set_ylabel(params[t].upper(), fontsize=14)

            fig.tight_layout()
        
            # reasonFolder = ""
            # reasonFolder = reason_map.get(itt_id[i], "") 
            # DataTypeFolder = "image_west"
            
            # for testing data             
            DataTypeFolder = "D:\\Nicole\\Laravel\\www\\public\\cott_images"
                        
            # print(x0 , '--x0')
            # print(x1 , '--x1')
            # print(x2 , '--x2')
            # print(x3 , '--x3')


                    
            # if reasonFolder == "" :
            #     reasonFolder = "CantBeMapped"
            
            # X軸(時間), 不需呈現
            # locator.MAXTICKS = 40000
            # ax[t].xaxis.set_major_locator(locator)

            plt.gcf().autofmt_xdate()
            date_format = mpl_dates.DateFormatter('%m-%d %H:00')
            hours = mpl_dates.HourLocator(interval = 6)
            plt.gca().xaxis.set_major_locator(hours)
            plt.gca().xaxis.set_major_formatter(date_format)
            # plt.xlabel('Time')
            plt.ylabel(params[t].upper())
            
            plt.gca().set_xlim(pd.to_datetime(df['EVENT_START_DATE'][0], format = '%Y-%m-%d %H:%M'),
                                pd.to_datetime(df['EVENT_DATE'][0], format = '%Y-%m-%d %H:%M'))
            
            # print('.\\'+DataTypeFolder+'\\'  +  itt_id[i] + '.png')
            # fig.savefig('.\\'+DataTypeFolder+'\\' + itt_id[i] + '.png')

            print(DataTypeFolder+'\\'  +  itt_id[i] + '.png')
            
            #資料不足,分開放, Today(訓練)、cott_images都不加入, 後再sftp上傳即可
            if x1 <= 10 or x2 <= 10 or x3 <= 10 :
                DataTypeFolder = DataTypeFolder + "_datainsufficient"
            else:
                fig.savefig(DataTypeFolder+'_today\\' + itt_id[i] + '.png')#上傳使用
                
            fig.savefig(DataTypeFolder+'\\' + itt_id[i] + '.png')

            # for testing data
            # print('./image_0705/' + itt_id[i] + '.png')
            # fig.savefig('./image_0705/' + itt_id[i] + '.png')
        
            # clear the image in memory and clear axes, and in order to reduce the memory occupation
            # plt.clf()
            # plt.close(fig)
            # plt.close('all')
            # del fig
            # if params[t]=='cell_rsrp' :
            #   plt.gca().invert_yaxis()
                            
        # plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei'] 
        # plt.rcParams['axes.unicode_minus'] = False
        # plt.title('客戶軌跡與網路訊號')
        except Exception as e:
                print('error')
                print(params[t])
                print(e)
                # continue
    
# del df_raw0
del df_raw
del df
del fig
# print ("\ngarbage len", len(gc.garbage))
# print ("garbages:", gc.garbage)
gc.collect()

# keep record time
now = datetime.now()
txt = 'generateImg.py, 上次更新時間,To：' + str(now)
df = pd.DataFrame([txt], index=['UpdateTime'])
df.to_csv('D:\\Nicole\\python\\cottCNN\\logCottCNN.csv', mode='a',header=False)
