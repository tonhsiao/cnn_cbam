from ftlib import FTP
import time
import tarfile

def ftpconnect(host,username,password):
    ftp = FTP()
    ftp.set_debuglevel(2)
    ftp.connect(host,21)
    ftp.login(username,password)
    
       
    
    return ftp

def downloadfile(ftp,remotepath,localpath):
    bufsize = 1024
    fp = open(localpath,'wb')
    ftp.retrbinary('RETR  '+ remotepath,fp.write,bufsize)
    # 接受服务器上文件并写入文本
    ftp.set_debuglevel(0) # 关闭调试
    fp.close() # 关闭文件


def uploadfile(ftp,remotepath,localpath):
    bufsize = 1024
    fp = open(localpath,'rb')
    # ftp.cwd(remotepath)
    # ftp.dir() 
    ftp.storbinary('STOR '+ remotepath, fp,bufsize) # 上传文件
    ftp.set_debuglevel(2)
    #ftp.quit()

import os
def ftp_upload(host,username,password,local,remote):
    ftp = ftpconnect(host,username,password)
    try:
      if os.path.isdir(local):#判斷是目錄還是檔案
        for f in os.listdir(local):#查看本地目錄
          uploadfile(ftp,os.path.join(remote+f), os.path.join(local+f))#上傳目錄中的檔案
      else:
        uploadfile(ftp,remote,local)#上傳檔案
    except Exception as e:
      print('upload exception:',e)
    ftp.close()
    
# if __name__ == "__main__":
    # ftp = ftpconnect("xxxxx","xxx","xxxxx")
    # downloadfile(ftp,"XXX","XXX")
    # uploadfile(ftp,"xxxx","xxxxx")

    # ftp.quit()