import re
import csv
import json
import numpy as np
import pandas as pd
import sys


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import col, split

if __name__=='__main__':
  sc = pyspark.SparkContext.getOrCreate()
  spark = SparkSession(sc)

  #prepare for key_food_products spark dataframe
  kf_pd = spark.read.load('keyfood_products.csv',format ='csv',header = True, inferSchema = False)
  kf_pd_split = kf_pd.select("store",
                            "department",
                            F.split(kf_pd.upc,"-").getItem(1).alias('upc_code'),
                            F.regexp_extract(kf_pd.price,"\d+\.\d+",0).alias('Price ($)')
                            )
  #prepare for key_food_sample_items spark dataframe
  kf_sp = spark.read.load('keyfood_sample_items.csv',format ='csv',header = True, inferSchema = False)
  kf_sp_split = kf_sp.select(
                            F.split(kf_sp['UPC code'],"-").getItem(1).alias('upc_code'),
                            kf_sp['Item Name'].alias('Item Name')
                            )
  kf_sp_m = kf_pd_split.join(kf_sp_split,['upc_code'],how = 'inner')
  #read json file
  js = open('keyfood_nyc_stores.json')
  store_dict= json.load(js)
  store_list = [[store_dict[x]['name'],store_dict[x]['foodInsecurity'],store_dict[x]['communityDistrict']] for x in store_dict]
  store_list_df = pd.DataFrame(store_list,columns=['store_name','foodInsecurity','communityDistrict'])
  store_list_spark = spark.createDataFrame(store_list_df)

  task_1 = kf_sp_m.join(store_list_spark,kf_sp_m.store ==store_list_spark.store_name,how='left' )
  task_1= task_1.withColumn('% Food Insecurity',(task_1['foodInsecurity']*100).cast('int'))
  #output task
  outputTask1 = task_1.select(task_1['Item Name'],task_1['Price ($)'].cast('float'),task_1['% Food Insecurity'])
  ## DO NOT EDIT BELOW
  outputTask1.write.csv(sys.argv[1])
