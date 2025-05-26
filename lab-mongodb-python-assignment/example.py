# coding=utf-8
import datetime
import time
import json
from pymongo import MongoClient
from faker import Faker


class Example:
    def data_generator(self, n):
        # Connect to MongoDB - Note: Change connection string as needed
        client = MongoClient('127.0.0.1:27017')
        db = client['test']
        # delete collection data if exists
        db.drop_collection("example")
        # create and obtain collection
        collection = db.create_collection('example')
        # Create sample data. Default languge for Faker object is 'en_US'. If wish you can specify other languages
        # such as spanish with 'en_US'
        fake = Faker(['it_IT', 'en_US'])
        for x in range(n):
            p = {"name": fake.name(), "address": fake.address(), "ssn": fake.ssn(), "birthdate": fake.date_time()}
            collection.insert_one(p)
            print(str(x+1) + ". Document inserted")
        # Get time at the start of the query
        start_time = time.time()
        result = collection.find_one()
        query_time = time.time()-start_time
        # Measure query execution time
        print("--- %s seconds ---" % (query_time) + str(result))
        client.close()
