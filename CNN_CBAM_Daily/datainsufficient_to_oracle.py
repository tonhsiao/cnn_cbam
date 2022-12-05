# -*- coding: utf-8 -*-
"""
Created on Fri Sep 30 09:34:48 2022

@author: noc
"""

import glob
import os

# 倒入ORACLE
import config as cfg
import cx_Oracle
def insert_orcl(cott, predict_type):
    sql = ('insert into nocadm.predict_cott(cott, predict_type) '
        'values(:cott,:predict_type) LOG ERRORS INTO nocadm.error_log_nois REJECT LIMIT UNLIMITED')
    try:
        # establish a new connection
        with cx_Oracle.connect(cfg.username,
                            cfg.password,
                            cfg.dsn,
                            encoding=cfg.encoding) as connection:
            # create a cursor
            with connection.cursor() as cursor:
                # execute the insert statement
                cursor.execute(sql, [cott, predict_type])
                # commit work
                connection.commit()
    except cx_Oracle.Error as error:
        print('Error occurred:')
        print(error)

dirPath = r"D:\Nicole\Laravel\www\public\cott_images_datainsufficient\*.png"
result = glob.glob(dirPath)

for f in result:
    if os.path.isfile(f) :
        #print(f.split('\\')[2])      #目錄名稱
        print(f.split("\\")[6][:-4]) #工單編號
        #寫入Oracle
        insert_orcl(f.split("\\")[6][:-4], "DataInsufficient") #工單編號,類別

# 上傳至web server
import myftp
local = 'D:/Nicole/Laravel/www/public/cott_images_datainsufficient/'
myftp.ftp_upload(cfg.webserver,cfg.webuser,cfg.webpass,local,cfg.webloc)

# 刪除PNG
import os 
for file in os.scandir(local):
    if file.name.endswith(".png"):
        os.unlink(file.path)
     

                 