import json
import random
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer
import pymongo
import os
import sys

#creating an instance of an Faker class
fake =Faker()

#kafka configuration
kafka_broker='localhost:9092'
producer =KafkaProducer(bootstrap_servers=[kafka_broker],value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# customers and products dataset
#customers = []
#products = []

#create connection to mongodb
#
#
client = pymongo.MongoClient("mongodb://192.168.1.14:27017/")

# connecting to database
database = client["ecommerce"]



# connecting to collections
products_collection = database["products"]
customers_collection=database["customers"]
transaction_collection=database["transactions"]
product_view_collection=database["product_views"]


customers_cursor = customers_collection.find({},{"customer_id":1,"_id":0})  # it will return cursor object
customers=[i["customer_id"] for i in customers_cursor]


products_cursor = products_collection.find({},{"product_id":1,"_id":0})  # it will return cursor object
products=[i["product_id"] for i in products_cursor]



v=0

try:
    print("Trying to fetch last transaction from transaction collection")
    last_transaction=product_view_collection.find({}).sort("view_id", pymongo.DESCENDING).limit(1)
    if last_transaction.count() != 0 :
        print("views are available in product_view collection")
        for i in last_transaction:
            #print(type(i))
            #print(i)
            #sys.exit(1)
            v = int(i["view_id"])
            print("last product_view's's view_id is :")
            print(v)
            #sys.exit(0)
    else:
        print("no product_view's available in product_view collection")
    '''
    for i in last_transaction:
        print(i["view_id"])
    '''
except Exception as e:
    print(e)
    v=0



def generate_product_view():
    global v
    v=v+1
    print(v)

    customer_id=random.choice(customers)
    product_id=random.choice(products)

    return  {
        "view_id":v,
        "customer_id":customer_id,
        "product_id":product_id,
        "cname":customers_collection.find({"customer_id":customer_id},{"_id":0,"name":1})[0]["name"],
        "pname": products_collection.find({"product_id": product_id}, {"_id": 0, "name": 1})[0]["name"],
        "category": products_collection.find({"product_id": product_id}, {"category": 1, "_id": 0})[0]["category"],
        "price": products_collection.find({"product_id": product_id}, {"_id": 0, "price": 1})[0]["price"],
        "supplier": products_collection.find({"product_id": product_id}, {"_id": 0, "supplier": 1})[0]["supplier"],
        "timestamp":fake.date_time_this_year().isoformat(),
        "view_duration":random.randint(1,4)
    }


while True:
    product_view=generate_product_view()
    producer.send(topic="product_views",value=product_view)
    time.sleep(5)


