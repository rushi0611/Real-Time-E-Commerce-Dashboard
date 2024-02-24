
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

print("starting")
#creating an instance of an Faker class
fake =Faker()

#kafka configuration
kafka_broker='localhost:9092'
producer =KafkaProducer(bootstrap_servers=[kafka_broker],value_serializer=lambda x: json.dumps(x).encode('utf-8'))



#create connection to mongodb

client = pymongo.MongoClient("mongodb://192.168.1.13:27017/")

# connecting to database
database = client["ecommerce"]

#print(database)

# connecting to collections
products_collection = database["products"]
customers_collection=database["customers"]
transaction_collection=database["transactions"]
product_view_collection=database["product_views"]




#fetching customers from customers collection in mongodb

customers_cursor = customers_collection.find({},{"customer_id":1,"_id":0})  # it will return cursor object
#print(customers_cursor)



customers=[i["customer_id"] for i in customers_cursor]
#print(customers)
#print(i)


#fetching products from products collection in mongodb

products_cursor = products_collection.find({},{"product_id":1,"_id":0})  # it will return cursor object
#print(products_cursor)



products=[i["product_id"] for i in products_cursor]
#print(products)


t=0

try:
    #print("Trying to fetch last transaction from transaction collection")
    #fetching last transaction from transactions collection
    last_transaction=transaction_collection.find({}).sort("transaction_id", pymongo.DESCENDING).limit(1)
    if last_transaction.count() != 0 :
        print("transactions are available in transactions collection")
        for i in last_transaction:
            #print(type(i))
            #print(i)
            #sys.exit(1)

            #fetching last transactions id
            t = int(i["transaction_id"])
            #print("last transaction's transaction_id is :")
            #print(t)
            #sys.exit(0)
    else:
        print("no transactions available in collection")

except Exception as e:
    print(e)
    t=0



countries=["India","England","Russia","France","Mexico","USA","Canada","China","Germany"]



def generate_transaction():
    global t
    t=t+1

    customer_id=random.choice(customers)
    product_id=random.choice(products)

    country=random.choice(countries)

    region=""
    if country=="India" or  country=="Russia" or country=="China":
        region="Asia"
    elif country=="Germany" or country=="France" or country=="England":
        region="Europe"
    elif country=="USA" or country=="Mexico" or country=="Canada" :
        region="North_America"





    return {
        "transaction_id": t,
        "customer_id": int(customer_id),
        "product_id": int(product_id),
        "pname":products_collection.find({"product_id":product_id},{"_id":0,"name":1})[0]["name"],
        "category":products_collection.find({"product_id":product_id},{"category":1,"_id":0})[0]["category"],
        "price":products_collection.find({"product_id":product_id},{"_id":0,"price":1})[0]["price"],
        "supplier":products_collection.find({"product_id":product_id},{"_id":0,"supplier":1})[0]["supplier"],
        "quantity": random.randint(1, 5),
        "date_time": fake.date_time_this_year().isoformat(),
        "status": random.choice(["completed", "pending", "canceled"]),
        "payment_method": random.choice(["credit card", "PayPal", "bank transfer"]),
        "country":country,
        "region": region

    }



cnt=0
check=random.randint(100,300)
while True:
    print('going inside generate_transaction')
    transaction=generate_transaction()
    print(transaction)
    producer.send(topic="transactions",value=transaction)
    time.sleep(2)



        
        







