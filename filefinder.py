#-*- coding: utf-8 -*-

########################################
# 功能说明：
# 1. 遍历特定类型的本地文件并记录数据库
# 2. 基于数据库记录向服务器上传文件并保证上传文件的唯一性
#
# 创建日期：
# 2016年 9月30日 星期五 11时49分23秒 CST
# 修改日期：
# 2016年10月 9日 星期日 23时30分39秒 CST
########################################
import os
import re
import hashlib
import sqlite3
import datetime
import ftplib
from random import Random

# 基本配置部分
# -------------------------------------------------------------
# 指定搜索路径（使用绝对路径）
SEARCH_DIR = "D:" + os.sep + "Documents"
SEARCH_DIR = "/Path/To/Your/Directory/"
# 指定搜索文件类型
FILE_TYPES = ["pdf", "ppt", "pptx", "docx", "doc"]
# 指定上传FTP服务器IP
FTP_SERVER_IP = '192.168.1.100'
# 指定动作（仅搜索；仅上传；搜索上传）
ACTION_SEARCH = 1
ACTION_UPLOAD = 0
# -------------------------------------------------------------

# 获取主机名
def getHostname():  
    sys = os.name  

    if sys == 'nt':  
        hostname = os.getenv('computername')
        return hostname
    
    elif sys == 'posix':  
        host = os.popen('echo $HOSTNAME')
        try:  
            hostname = host.read().strip('\n')
            return hostname
        finally:
            host.close()
    else:
        str = ''
        chars = 'AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz0123456789'
        length = len(chars) - 1
        random = Random()
        for i in range(20):
            str += chars[random.randint(0, length)]
        return str


## 计算文件的MD5
def getFileMd5(filename):
    if not os.path.isfile(filename):
        return
    myhash = hashlib.md5()
    f = open(filename,'rb')
    while True:
        b = f.read(8096)
        if not b:
            break
        myhash.update(b)
    f.close()
    return myhash.hexdigest()

def searchFilesToSqlite(dirPath, suffixs):
    '''函数说明：遍历目录文件并存储文件md5和路径等信息到Sqlite数据库'''
    fileDict = dict() 
    for root, dirs, files in os.walk(dirPath):
        for fileObj in files:

            if fileObj.startswith('~'):
                continue
            
            fileType = os.path.splitext(fileObj)[1][1:]
            if fileType in suffixs:
                fileFullpath = os.path.join(root, fileObj)
                print ("[GET] " + fileFullpath)
                fileMd5 = getFileMd5(fileFullpath)
                fileDict.setdefault(fileMd5, [fileFullpath, fileType])
    
    conn = sqlite3.connect("filefinder.db")
    #############################
    # 避免如下错误，加入：conn.text_factory = str
    # ProgrammingError: You must not use 8-bit bytestrings 
    # unless you use a text_factory that can interpret 8-bit bytestrings (like text_factory = str). 
    # It is highly recommended that you instead just switch your application to Unicode strings.
    #############################
    
    conn.text_factory = str
    c = conn.cursor()

    #############################
    # 数据库表filecategory的说明
    # status: [0] new file, not upload; [1] uploaded
    # file_type: pdf, ppt, pptx, doc, docx ...
    #############################

    c.execute('''CREATE TABLE IF NOT EXISTS filecategory
    (file_md5 text UNIQUE, file_path text, file_type, date_inserted text,
    date_uploaded text, status integer)''')

    insert_stmt = 'INSERT INTO filecategory VALUES(?, ?, ?, ?, ?, ?)'
    
    for key in fileDict.keys():
        date_inserted = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        record = (key, fileDict[key][0], fileDict[key][1], date_inserted, '', 0)
        try:
            print ("[INSERTING] Record [%s]" %(key))
            c.execute(insert_stmt, record)
            conn.commit()
        except sqlite3.IntegrityError as e:
            print ("[EXIST] Record [%s] already exists in the database." %(key))
    conn.close()
    return
    
def uploadFileToFTP(server_ip):
    '''注意：要在本地数据库中对已经上传过的文件status置1；
    判断本地数据库文件是否存在；
    上传至FTP服务器，需配置FTP允许匿名上传文件
    '''

    # Open FTP
    ftp = ftplib.FTP(server_ip)
    ftp.login()
    dir_hostname = getHostname()

    # 判断FTP指定目录是否存在，否则创建并进入该目录
    try:
        ftp.cwd(dir_hostname)
    except ftplib.error_perm:
        try:
            ftp.mkd(dir_hostname)
            ftp.cwd(dir_hostname)
        except ftplib.error_perm:
            print ('[Error] You have no authority to make dir!')

    if not os.path.exists("filefinder.db"):
        print("[Warning] The Database file is missing! You must execute file searching firstly.")
        return
    
    conn = sqlite3.connect("filefinder.db")
    conn.text_factory = str
    c = conn.cursor()
    query_stmt = 'SELECT * FROM filecategory;'
    c.execute(query_stmt)
    rs = c.fetchall()
    for item in rs:
        file_md5 = item[0]
        file_path = item[1]
        file_ext = item[2]
        status = item[5]
        
        if status == 1:
            print ("[STATUS] File [%s] has been uploaded." % (file_md5))
            continue
        
        print ("[Uploading] " + file_path)
        
        if not os.path.exists(file_path):
            print("[Warning] The file is removed but it's record is left behind in db!")
            c.execute("DELETE FROM filecategory WHERE file_md5 =" + file_md5)
            conn.commit()
            continue
        
        ff = open(file_path,'rb') # file to send
        buffersize = 2048
        ftp.storbinary('STOR '+ file_md5 + '.' + file_ext, ff, buffersize) # send the file
        ff.close() #close file
        date_uploaded = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        c.execute("UPDATE filecategory SET status = 1 , date_uploaded = '%s' WHERE file_md5 = '%s'" % (date_uploaded, file_md5))
        conn.commit()
    conn.close()
    ftp.quit()
    return
    
## ----------------------------------------------------
def main():
    if ACTION_SEARCH:
        searchFilesToSqlite(SEARCH_DIR, FILE_TYPES)
        
    if ACTION_UPLOAD:
        uploadFileToFTP(server_ip = FTP_SERVER_IP)

if __name__ == '__main__':
    main()
