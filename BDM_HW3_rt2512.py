import csv
import json
import numpy as np
import matplotlib.pyplot as plt

import pandas as pd



import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import types as T
from functools import reduce
from pyspark.sql import DataFrame

from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("App")
conf = (conf.setMaster('local[*]')
        .set('spark.executor.memory', '8G')
        .set('spark.driver.memory', '45G')
        .set('spark.driver.maxResultSize', '25G'))
sc = SparkContext(conf=conf)
sc = pyspark.SparkContext.getOrCreate()
spark = SparkSession(sc)
spark

import sys

if __name__=='__main__':
        item = 'keyfood_sample_items.csv'
        product = 'keyfood_products.csv'
        foodInsecurity = 'keyfood_nyc_stores.json'

        dfkeyfood_items = spark.read.load(item, format='csv', header=True, inferSchema=True)

        dfkeyfood_items = dfkeyfood_items.withColumn("upc_code", split(dfkeyfood_items['UPC Code'], "-").getItem(1))

        dfkeyfood_product = spark.read.load(product, format='csv', header=True, inferSchema=True)

        dfkeyfood_product = dfkeyfood_product.withColumn('price_01',split(dfkeyfood_product['price'], "\xa0").getItem(0))

        dfkeyfood_product = dfkeyfood_product.withColumn('price_02',substring('price_01', 2, 5))

        dfkeyfood_product = dfkeyfood_product.drop("price","price_01")
        dfkeyfood_product = dfkeyfood_product.select('store','department','upc','product','size',dfkeyfood_product['price_02']\
                                                     .alias('price').cast('float'))

        dfkeyfood_product = dfkeyfood_product.withColumn("upc_code01", split(dfkeyfood_product.upc, "-").getItem(1))

        df = dfkeyfood_product.join(dfkeyfood_items,dfkeyfood_product.upc_code01==dfkeyfood_items.upc_code,how="inner")

        df = df.select("store", "product","price","upc_code","department")

        dfStorelist = df.select(df['store']).distinct()
        store_list = []
        for col in dfStorelist.collect():
          store_list.append(col[0])
        json_df = spark.read.json(foodInsecurity, multiLine=True)

        df_list=[]

        for storeName in store_list:
          appendlist = json_df.select("{}.*".format(storeName)).select("name", "foodInsecurity")
          df_list.append(appendlist)

        dfList = reduce(DataFrame.unionAll, df_list)

        df = df.join(dfList,df.store==dfList.name,how="inner")

        outputTask1 = df.select(df['product'].alias('Item Name'),df['price'].alias('Price ($)'),df['foodInsecurity'].cast('float'))

        outputTask1 = outputTask1.withColumn('% Food Insecurity', (outputTask1.foodInsecurity*100).cast('int')).drop("foodInsecurity")
        outtask =outputTask1.cache()
        outtask.write.csv(sys.argv[1])
