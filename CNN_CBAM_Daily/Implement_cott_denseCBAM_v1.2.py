# -*- coding: utf-8 -*-
"""
Created on 2022/9/25
@author: nhsiao
"""
# import matplotlib.pyplot as plt
from tqdm import tqdm
import numpy as np
import pandas as pd
import os
import cv2
from tensorflow.keras.models import Model, load_model
import warnings
warnings.filterwarnings('ignore')
import func
import config as cfg

version = '1_2'
from datetime import datetime, timedelta

today = datetime.today().strftime("%Y-%m-%d")

# keep process time
now = datetime.now()
txt = 'Implement_cottdenseCBAM_v.2.py, 上次更新時間,From：' + str(now)
df = pd.DataFrame([txt], index=['UpdateTime'])
df.to_csv('D:\\Nicole\\python\\cottCNN\\logCottCNN.csv', mode='a', header=False)

# get images
data_path = "D:\\Nicole\\Laravel\\www\public\\cott_images_today"
x_test_list = []
cott_list = []
for roots, dirs, files in os.walk(data_path):
    for each in files:
        if each.find('checkpoint') == -1:
            x_test_list.append(os.path.join(roots, each))
            cott_list.append(each[:-4]) #檔名

img_size = 224

def load_img(data_list):
    data_img = []
    for each in tqdm(data_list):
        img = cv2.imread(each, 1)
        img = cv2.resize(img, (img_size, img_size))
        data_img.append(img[..., np.newaxis])

    return np.array(data_img).astype('float32')/255.
x_test = load_img(x_test_list)
x_test = np.squeeze(x_test)

# predict
model = load_model('./basic_model-denseCBAM_v1_2.h5')
predicted = model.predict(x_test)
predicted = np.argmax(predicted, 1)

# 組合工單編號
predict_submission = pd.DataFrame({'cott':cott_list, 'predict_reason_code': predicted})

class_map = pd.read_csv('./class_mapping_v1.txt',header=None, index_col=0)
class_map = class_map.to_dict()[1]

predict_submission['predict_reason'] = predict_submission['predict_reason_code'].map(class_map).values.copy()

class_names = np.array([each for each in class_map.values()])
print(class_names)

siteid_map = pd.read_csv('./sitelist.csv',header=None, index_col=0)
siteid1_map = siteid_map.to_dict()[1]
siteid2_map = siteid_map.to_dict()[2]
siteid3_map = siteid_map.to_dict()[3]
siteid1_dis_map = siteid_map.to_dict()[4]
siteid2_dis_map = siteid_map.to_dict()[5]
siteid3_dis_map = siteid_map.to_dict()[6]

predict_submission['site1'] = predict_submission['cott'].map(siteid1_map).values.copy()
predict_submission['site2'] = predict_submission['cott'].map(siteid2_map).values.copy()
predict_submission['site3'] = predict_submission['cott'].map(siteid3_map).values.copy()
predict_submission['site1_dis'] = predict_submission['cott'].map(siteid1_dis_map).values.copy()
predict_submission['site2_dis'] = predict_submission['cott'].map(siteid2_dis_map).values.copy()
predict_submission['site3_dis'] = predict_submission['cott'].map(siteid3_dis_map).values.copy()

predict_submission['site1'].fillna(value='-', inplace=True)
predict_submission['site2'].fillna(value='-', inplace=True)
predict_submission['site3'].fillna(value='-', inplace=True)
predict_submission['site1_dis'].fillna(value='-', inplace=True)
predict_submission['site2_dis'].fillna(value='-', inplace=True)
predict_submission['site3_dis'].fillna(value='-', inplace=True)

predict_submission['predict_reason'].value_counts()

# 倒入ORACLE
for i, j in predict_submission.iterrows():
    print(j['cott'], j['predict_reason'], j['site1'], j['site2'], j['site3']) 
    func.insert_orcl_ori(j['cott'], j['predict_reason'], j['site1'], j['site2'], j['site3'], j['site1_dis'], j['site2_dis'], j['site3_dis']) 
    

# 上傳至web server
import myftp
local = 'D:/Nicole/Laravel/www/public/cott_images_today/'
myftp.ftp_upload(cfg.webserver,cfg.webuser,cfg.webpass,local,cfg.webloc)
  

# 刪除PNG
import os 
for file in os.scandir(local):
    if file.name.endswith(".png"):
        os.unlink(file.path)

# keep process time
now = datetime.now()
txt = 'Implement_cottdenseCBAM_v.2.py, 上次更新時間,To: ' + str(now)
df = pd.DataFrame([txt], index=['UpdateTime'])
df.to_csv('D:\\Nicole\\python\\cottCNN\\logCottCNN.csv', mode='a', header=False)

os.unlink("D:\\Nicole\\python\\cottCNN\\sitelist.csv")

