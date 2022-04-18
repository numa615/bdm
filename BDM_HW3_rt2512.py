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

#<YOUR CODE HERE>
keyfoodproducts = spark.read.load('/tmp/bdm/keyfood_products.csv',format ='csv',header = True, inferSchema = False)
keyfood_stores = keyfoodproducts.select("store",
                            "department",
                            F.split(keyfoodproducts.upc,"-").getItem(1).alias('upc_code'),
                            F.regexp_extract(keyfoodproducts.price,"\d+\.\d+",0).alias('price')
                            )
keyfoodsplits = spark.read.load('keyfood_sample_items.csv',format ='csv',header = True, inferSchema = False)
keyfood_split = keyfoodsplits.select(
                            F.split(keyfoodsplits['UPC code'],"-").getItem(1).alias('upc_code'),
                            keyfoodsplits['Item Name'].alias('itemName')
                            )
keyfoodsplit1 = keyfood_split.join(keyfood_stores,['upc_code'],how = 'left')
with open('keyfood_nyc_stores.json') as js:
    store_dict= json.load(js)
    js.close()
list_stores = [[store_dict[x]['name'],store_dict[x]['foodInsecurity'],store_dict[x]['communityDistrict']] for x in store_dict]
list_stores_df = pd.DataFrame(list_stores,columns=['store_name','foodInsecurity','communityDistrict'])
list_stores_sp = spark.createDataFrame(list_stores_df)

task1 = keyfoodsplit1.join(list_stores_sp,keyfoodsplit1.store ==list_stores_sp.store_name,how='left' )
outputTask1 = task1.select(task1['itemName'].alias('Item Name'),
                              task1['price'].alias('Price ($)').cast('float'),
                              (task1['foodInsecurity']*100).alias('% Food Insecurity').cast('int')
  )
## DO NOT EDIT BELOW
outputTask1 = outputTask1.cache()
outputTask1.write.csv(sys.argv[1])


