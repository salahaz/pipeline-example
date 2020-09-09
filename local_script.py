import difflib
import psycopg2
import pandas as pd
import numpy as np
import re

# connect to the database (Local or Remote)
connection = psycopg2.connect(
    host="localhost",
    database="csc3350",
    user="salahaz",
    password="@csc3350@")
cursor = connection.cursor()

# Open Etcpasswd and Etcshadow Textfiles
filePath1 = "etcpasswd.txt"
filePath2 = "etcshadow.txt"
textFile1 = open(filePath1, 'r').read().splitlines()
textFile2 = open(filePath2, 'r').read().splitlines()
finalText1, finalText2 = [], []
username, passwd, userId, groupId, gecos, homeDirectory, shell = [], [], [], [], [], [], []
user_name, encryptPwd, lastUpdate, minDays, maxDays, nbExpMess, nbDisableAcc, expDate, reserve = [], [], [], [], [], [], [], [], []

# Prepare Data for Inner Join 
for line in textFile1:
    tmp = line.split(':')
    username.append(tmp.pop(0))
    passwd.append(tmp.pop(0))
    userId.append(tmp.pop(0))
    groupId.append(tmp.pop(0))
    gecos.append(tmp.pop(0))
    homeDirectory.append(tmp.pop(0))
    shell.append(tmp.pop(0))
    line = ''.join(re.sub("\s", '', str(line)))
    finalText1.append(line.strip())
d1 = {'username':pd.Series(username), 'passwd':pd.Series(passwd), 'userId':pd.Series(userId)
      , 'groupId':pd.Series(groupId), 'gecos':pd.Series(gecos), 'homeDirectory':pd.Series(homeDirectory)
      , 'shell':pd.Series(shell)}
df1 = pd.DataFrame(d1)

for line in textFile2:
    tmp = line.split(':')
    user_name.append(tmp.pop(0))
    encryptPwd.append(tmp.pop(0))
    lastUpdate.append(tmp.pop(0))
    minDays.append(tmp.pop(0))
    maxDays.append(tmp.pop(0))
    nbExpMess.append(tmp.pop(0))
    nbDisableAcc.append(tmp.pop(0))
    expDate.append(tmp.pop(0))
    reserve.append(tmp.pop(0))
    line = ''.join(re.sub("\s", '', str(line)))
    finalText2.append(line.strip())
d2 = {'username':pd.Series(user_name), 'encryptPwd':pd.Series(encryptPwd), 'lastUpdate':pd.Series(lastUpdate)
      , 'minDays':pd.Series(minDays), 'maxDays':pd.Series(maxDays), 'nbExpMess':pd.Series(nbExpMess)
      , 'nbDisableAcc':pd.Series(nbDisableAcc), 'expDate':pd.Series(expDate), 'reserve':pd.Series(reserve)}
df2 = pd.DataFrame(d2)

df3 = pd.merge(df1, df2, on="username", how="inner")



#print(df3[['username', 'encryptPwd']])
# Create jointable on SQL and store results
cursor.execute("SELECT * FROM information_schema.tables WHERE table_name='jointable'")
if not bool(cursor.rowcount):
    cursor.execute("CREATE TABLE jointable(\
                   username VARCHAR(30),\
                   pwd VARCHAR(15),\
                   encryptPwd VARCHAR(100),\
                   PRIMARY KEY (username))")
    connection.commit();
    for index, rows in df3.iterrows():
        dataList = [rows.username, rows.passwd, rows.encryptPwd]
        cursor.execute("INSERT INTO jointable(username, pwd, encryptPwd) VALUES(%s,%s,%s)", (dataList[0], dataList[1], dataList[2]))
        connection.commit();

# Checking files for updates
else:
    # Retrieving and preparing DB data for comparison
    cursor.execute("SELECT * FROM jointable")
    databaseData = cursor.fetchall()
    finalData = []
    for line in databaseData:
        line1 = ''.join(re.sub("\(|\)|\'|\s", '', str(line)))
        line1 = ''.join(re.sub("\,", ":", line1))
        finalData.append(line1)
            
    # Preparing previous join data for comparison
    rowsList = []
    for index, rows in df3.iterrows():
        dataList = [rows.username, rows.passwd, rows.encryptPwd]
        line = ''.join(re.sub("\[|\]|\'|\s", '', str(dataList)))
        line = ''.join(re.sub("\,", ":", line))
        rowsList.append(line)

    # Comparing both data for any updates
    diffInstance = difflib.Differ()
    diffList = list(diffInstance.compare(rowsList, finalData))
    for line in diffList:
        if line[0] == '-':
           line = ''.join(re.sub("\-|\s", '', str(line)))
           line = line.split(':')
           cursor.execute("INSERT INTO jointable(username, pwd, encryptPwd) VALUES(%s,%s,%s)", (line[0], line[1], line[2]))
           connection.commit();
           
           
           

