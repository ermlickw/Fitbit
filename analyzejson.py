import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import sys
import os
import sqlite3
import json
from pandas.io.json import json_normalize
#make DB and write output
from pymongo import MongoClient

datafolderholder = r'C:\Users\BillyErmlick\Desktop\Workspace\Python\Fitbit\MyFitbitData\WilliamErmlick\user-site-export'
datafolder = os.path.realpath(datafolderholder) #find directory where data is stored


client = MongoClient('localhost', 27017)
db = client['Fitbit_db']

types = ['sedentary_minutes', 'sleep','moderately_active',
        'altitude','weight','lightly_active','heart_rate','resting_heart',
        'badge','calories','demographic_vo2_max','distance','exercise',
        'run_vo2_max','steps','time_in_heart_rate_zones','very_active_minutes']

#sedentary_minutes
for dtype in types:
    collection_name = db[dtype]
    i=0
    for root, dirs, files in os.walk(datafolder): #walk it
        for file in files:
            if file.endswith(".json") and file.startswith(dtype):
                print(file)
                data= pd.read_json((os.path.join(datafolder, file)))
                with open((os.path.join(datafolder, file))) as f:
                    file_data = json.load(f)
                i+=1
                collection_name.insert_many(file_data)


    print('Inserted ' + str(i) + ' files into the '+ dtype+ ' collection.')


client.close()