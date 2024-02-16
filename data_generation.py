import json
import random
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer
import pymongo
import os

#creating an instance of an Faker class
fake =Faker()

#kafka configuration
kafka_broker='localhost:9092'
producer =KafkaProducer(bootstrap_servers=[kafka_broker],value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# customers and products dataset
customers = []
products = []

#create connection to mongodb
#
#
client = pymongo.MongoClient("mongodb://localhost:27017/")

# connecting to database
database = client["ecommerce"]

# connecting to collections
products_collection = database["products"]
customers_collection=database["customers"]
transaction_collection=database["transactions"]




#here there is worst case, ask about these to anay sir

#checking if collection contains records, if contains then fetching last documnets's product_id value and then cretaing new documents
#product id accordingly

c=0

try:
    last_document=products_collection.find().sort({"_id":-1}).limit(1)
    c=last_document.product_id
except Exception as e:
    c=0




# Generate Customer Data (it is data of master database i.e it doesn't changes exponentially)
def generate_customer():
    global c
    c+=1

    name=fake.name()
    namearr=name.split(' ')
    customer = {
        "customer_id":'C'+str(c),
        "name": name,
        "email": namearr[0]+namearr[1]+str(random.randint(1,1000))+str(random.randint(2,9))+'@gmail.com',
        "location": fake.address(),
        "age": random.randint(18, 70),
        "gender": random.choice(["Male", "Female", "Other"]),
        "account_created": fake.past_date().isoformat(),
        "last_login": fake.date_time_this_month(()).isoformat()
    }
    customers.append(customer["customer_id"])
    #print(customer)


    # adding document generated above to collection
    try:
        x = customers_collection.insert_one(customer)
        #print(x.inserted_id)
        print(x.acknowledged)
    except Exception as e:
        print(e)
        exit()

    return customer

#Generate product data  (it is   data of master database i.e it doesn't changes exponentially)

#checking if collection contains records, if contains then fetching last documnets's customer_id value and then cretaing new document's
#customer_id accordingly

p=0

try:
    last_document=products_collection.find().sort({"_id":-1}).limit(1)
    p=last_document.product_id
except Exception as e:
    p=0


def generate_product():
    global p
    p+=1
    categories=['Electronics', 'Books', 'Clothing', 'Home & Garden']
    product={
        'product_id':'P'+str(p),
        'name':fake.word().title(),
        'category':random.choice(categories),
        'price':round(random.uniform(10,500),2),
        'supplier':fake.company(),
        'rating':round(random.uniform(1,5),1)
    }

    products.append(product['product_id'])
    #print(product)

    # adding document generated above to collection
    try :
        x = products_collection.insert_one(product)
        #print(x.inserted_id)
        print(x.acknowledged)
    except Exception as e:
        print(e)
        exit()

    return product



# create transaction datastream based on using data from mongo db




#generate transaction data

t=0

try:
    last_transaction=transaction_collection.find().sort({"_id":-1}).limit(1)
    t=last_transaction.transaction_id
except Exception as e:
    t=0



def generate_transaction():
    global t
    t=t+1

    customer_id=random.choice(customers)
    product_id=random.choice(products)
    return {
        "transaction_id": t,
        "customer_id": customer_id,
        "product_id": product_id,
        "quantity": random.randint(1, 5),
        "date_time": fake.date_time_this_year().isoformat(),
        "status": random.choice(["completed", "pending", "canceled"]),
        "payment_method": random.choice(["credit card", "PayPal", "bank transfer"])
    }








# commenting temporarily


'''


# Generate Product View Data
def generate_product_view():
    return {
        "view_id": fake.uuid4(),
        "customer_id": random.choice(customers),
        "product_id": random.choice(products),
        "timestamp": fake.date_time_this_year().isoformat(),
        "view_duration": random.randint(10, 300)  # Duration in seconds
    }

# Generate System Log Data
def generate_system_log():
    log_levels = ["INFO", "WARNING", "ERROR"]
    return {
        "log_id": fake.uuid4(),
        "timestamp": fake.date_time_this_year().isoformat(),
        "level": random.choice(log_levels),
        "message": fake.sentence()
    }

# Generate User Interaction Data
def generate_user_interaction():
    interaction_types = ["wishlist_addition", "review", "rating"]
    return {
        "interaction_id": fake.uuid4(),
        "customer_id": random.choice(customers),
        "product_id": random.choice(products),
        "timestamp": fake.date_time_this_year().isoformat(),
        "interaction_type": random.choice(interaction_types),
        "details": fake.sentence() if interaction_types == "review" else None
    }


'''


def send_data():
    # Occasionally add new customers or products

    customer = generate_customer()
    #producer.send('ecommerce_customers', value=customer)
    product = generate_product()
    #producer.send('ecommerce_products', value=product)



    # Higher chance to create transactions and interactions
    if customers and products:
        transaction = generate_transaction()
        print(transaction)
        producer.send('ecommerce_transactions', value=transaction)
'''
        product_view = generate_product_view()
        if product_view:
            producer.send('ecommerce_product_views', value=product_view)
        user_interaction = generate_user_interaction()
        if user_interaction:
            producer.send('ecommerce_user_interactions', value=user_interaction)
            
          
            
        
    producer.send('ecommerce_system_logs', value=generate_system_log())
    

'''
# Parallel Data Generation






with ThreadPoolExecutor(max_workers=5) as executor:
    while True:
        executor.submit(send_data)
        time.sleep(random.uniform(0.01, 0.1))






