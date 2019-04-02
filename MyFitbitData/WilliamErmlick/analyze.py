import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import sys
import os
import sqlite3
import json
from pandas.io.json import json_normalize

datafolderholder = r'C:\Users\BillyErmlick\Desktop\Workspace\Python\Fitbit\MyFitbitData\WilliamErmlick\user-site-export'
datafolder = os.path.realpath(datafolderholder) #find directory where data is stored

#make DB and write output
conn = sqlite3.connect('Fitbit.db')
c = conn.cursor()
#
# #sedentary_minutes
# dtype = 'sedentary_minutes'
# try:
#     table_name = dtype
#     c.execute('''CREATE TABLE %s (
#                 DateTime timestamp,
#                 Hours real)''' %
#                 (table_name))
# except:
#     c.execute("Delete from %s" % (table_name)) #delete table data if already created
#
#
# for root, dirs, files in os.walk(datafolder): #walk it
#     for file in files:
#         if file.endswith(".json") and file.startswith(dtype):
#             print(file)
#             data= pd.read_json((os.path.join(datafolder, file)))
#             data.iloc[:,0]=data.iloc[:,0].apply(lambda x: str(x))
#             data.iloc[:,1]=data.iloc[:,1].apply(lambda x: x/60) #convert to hours
#
#             c.executemany('insert into %s values (?,?)' % (table_name), data.values.tolist())
# conn.commit()
# #print count
# c.execute('select count(*) from %s'% (table_name))
# result = c.fetchone()
# print('Inserted ' + str(result[0])+ ' records into the '+ table_name+ ' table.')
#
# #######steps
# dtype = 'steps'
# try:
#     table_name = dtype
#     c.execute('''CREATE TABLE %s (
#                 DateTime timestamp,
#                 Steps real)''' %
#                 (table_name))
# except:
#     c.execute("Delete from %s" % (table_name)) #delete table data if already created
#
#
# for root, dirs, files in os.walk(datafolder): #walk it
#     for file in files:
#         if file.endswith(".json") and file.startswith(dtype):
#             print(file)
#             data= pd.read_json((os.path.join(datafolder, file)))
#             data.iloc[:,0]=data.iloc[:,0].apply(lambda x: str(x))
#
#             c.executemany('insert into %s values (?,?)' % (table_name), data.values.tolist())
# conn.commit()
# #print count
# c.execute('select count(*) from %s'% (table_name))
# result = c.fetchone()
# print('Inserted ' + str(result[0])+ ' records into the '+ table_name+ ' table.')
#



# #######weight
# dtype = 'weight'
# try:
#     table_name = dtype
#     c.execute('''CREATE TABLE %s (
#                 BMI real,
#                 DATE text,
#                 FAT real,
#                 ID real,
#                 SOURCE text,
#                 TIME text,
#                 WEIGHT real)''' %
#                 (table_name))
# except:
#     c.execute("Delete from %s" % (table_name)) #delete table data if already created
#
#
# for root, dirs, files in os.walk(datafolder): #walk it
#     for file in files:
#         if file.endswith(".json") and file.startswith(dtype):
#             print(file)
#             data= pd.read_json((os.path.join(datafolder, file)))
#             data.iloc[:,1]=data.iloc[:,1].apply(lambda x: str(x)) #timestamp to string
#
#             c.executemany('insert into %s values (?,?,?,?,?,?,?)' % (table_name), data.values.tolist())
# conn.commit()
# #print count
# c.execute('select count(*) from %s'% (table_name))
# result = c.fetchone()
# print('Inserted ' + str(result[0])+ ' records into the '+ table_name+ ' table.')

#######sleep
dtype = 'sleep'
try:
    table_name = dtype
    c.execute('''CREATE TABLE %s (
                dateofSleep timestamp,
                duration int,
                efficiency int,
                endtime text,
                infocode text,
                data text,
                shortdata text,
                asleep_count int,
                asleep_min int,
                awake_count int,
                awake_min int,
                deep_count int,
                deep_minutes int,
                deep_thirtydayavg int,
                light_count int,
                light_minutes int,
                light_thirtydayavg int,
                rem_count int,
                rem_minutes int,
                rem_thirtydayavg int,
                restless_count int,
                restless_minutes int,
                wake_count int,
                wake_minutes int,
                wake_thirtydayavg int,
                logid int,
                minutesAfterWakeup int,
                minutesAsleep int,
                minutesAwake int,
                minutesToFallAsleep int,
                startTime text,
                timeInBed int,
                type text)''' %
                (table_name))
except:
    c.execute("Delete from %s" % (table_name)) #delete table data if already created


for root, dirs, files in os.walk(datafolder): #walk it
    for file in files:
        if file.endswith(".json") and file.startswith(dtype):
            print(file)
            with open(os.path.join(datafolder, file)) as f:
                d = json.load(f)
            data = json_normalize(d)
            data.iloc[:,1]=data.iloc[:,1].apply(lambda x: str(x)) #timestamp to string
            data.iloc[:,5]=data.iloc[:,5].apply(lambda x: str(x)) #array to string
            data.iloc[:,6]=data.iloc[:,6].apply(lambda x: str(x)) #array to string
            # print(data.iloc[1,:])
            # print(len(data.iloc[1,:]))
            if len(data.iloc[1,:]) == 33: #if full records
                c.executemany('insert into %s values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)' % (table_name), data.values.tolist())
conn.commit()
#print count
c.execute('select count(*) from %s'% (table_name))
result = c.fetchone()
print('Inserted ' + str(result[0])+ ' records into the '+ table_name+ ' table.')

#
# data = pd.read_json('MyFitbitData/WilliamErmlick/user-site-export/sedentary_minutes-2018-02-07.json')
# sns.scatterplot(y=data['dateTime'], x=data['value']/60, )
# plt.ylim(data['dateTime'][0],data['dateTime'][len(data)-1])
# plt.xlim(0,max(data['value']*1.3/60))
