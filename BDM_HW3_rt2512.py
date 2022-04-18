from pyspark import SparkContext
import csv
import json

if __name__=='__main__':
  sc = pyspark.SparkContext.getOrCreate()
  spark = SparkSession(sc)
  
  items = 'keyfood_sample_items.csv'
  stores = 'keyfood_nyc_stores.json'
  products = '/tmp/bdm/keyfood_products.csv'

  # upc_name from items
  dfitems = spark.read.load(items, format='csv', header=True, inferSchema=True)
  upc_name = dfitems.select(
      F.split(dfitems['UPC code'],"-")[1].alias('UPC'),
      dfitems['Item Name']
      )

  # store_fooinsecurity from items
  with open('keyfood_nyc_stores.json') as js:
    store_dict = json.load(js)
    js.close
  stores = [[store_dict[x]['name'],store_dict[x]['foodInsecurity']*100] for x in store_dict]
  df_stores = pd.DataFrame(stores,columns=['store_name','% Food Insecurity'])
  store_fooinsecurity = spark.createDataFrame(df_stores)

  # store_item_price from products
  dfproducts = spark.read.load(products, format='csv', header=True, inferSchema=True)
  store_item_price = dfproducts.select('store', 
                                      F.split(dfproducts.upc,'-')[1].alias('UPC'),
                                      F.regexp_extract(dfproducts.price,"\d+\.\d+",0).alias('Price($)').cast('float')
                              )
  # join
  output1 = upc_name.join(store_item_price,['UPC'],how = 'left')
  output2 = output1.join(store_fooinsecurity,output1.store ==store_fooinsecurity.store_name,how='left' )
  outputTask1 = output2[['Item Name','Price($)','% Food Insecurity']]
  # outputTask1.show()

  ## DO NOT EDIT BELOW
  outputTask1.write.csv(sys.argv[1])
