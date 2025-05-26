# coding=utf-8
import datetime
import time
import json
from pymongo import MongoClient
from faker import Faker

# M1: Two types of documents, one for each class and referenced fields.

class Model1:
	def data_generator(self, n):
		# Connect to MongoDB
		client = MongoClient("mongodb://localhost:27017/")
		db = client["test"]
		# Delete collection data if exists:
		db.drop_collection("m1_persons")
		db.drop_collection("m1_companies")
		# Creating two Python references to the MongoDB collections (persons and companies):
		persons = db.create_collection("m1_persons")
		companies = db.create_collection("m1_companies")
		# Generate fake names, companies, adresses etc.
		# We are using only the US, so it is clean and comparable
		fake = Faker(['en_US'])
		# COMPANIES
		# we will generate the following number of companies:
		n_companies = int(n // 500)
		# we need a list to to store the ObjectIds returned from MogoDB when inserting companies:
		company_ids = []
		# Next we generate and insert:
		for _ in range(n_companies):
			# We generate a company with all the attributes from the UML:
			company = {
				"domain": fake.domain(), 
				"email": fake.email(), 
				"name": fake.company(), 
				"url": fake.url(),
				"vatNumber": fake.vat_id()
				}
			# Insert the company into the collection and store the ObjectId:
			company_id = companies.insert_one(company).inserted_id
			# Append the ObjectId to the list:
			company_ids.append(company_id)
		# PERSONS
		# we will generate the following number of persons 
		# (so the total number of persons is for example 50,000 - 100 = 49,900):
		n_persons = n - n_companies
		# Next we generate and insert:
		for i in range(n_persons):
			# we get fake birth year between 1950 and 2000:
			birth_date = fake.date_of_birth(minimum_age=25, maximum_age=75)
			# We generate a person with all the attributes from the UML:
			person = {
				"age": 2025 - birth_date.year, 
				"companyEmail": fake.email(), 
				"dateOfBirth": birth_date.strftime("%Y-%m-%d"),
				"firstName": fake.first_name(),
				"lastName": fake.last_name(),
				"sex": fake.random_element(elements=("M", "F"))
				}
			# We randomly select a company from the list of ObjectIds:
			person["company"] = company_ids[i % n_companies]
			# Insert the person into the collection:
			persons.insert_one(person)
			print(str(i + 1 + n_companies) + ". Document inserted")
			
		# Close the MongoDB connection:
		client.close()
		return
	
	# QUERY 1
	# For each person, retrieve their full name and their company’s name.

	def query_q1(self):
		client = MongoClient("mongodb://localhost:27017/")
		db = client["test"]
		persons = db["m1_persons"]
		# Get time at the start of the query:
		start_time = time.time()
		
		# We use the aggregation framework to join the two collections:
		results = persons.aggregate([
			{
				# manually joining the two collections with $lookup:
				"$lookup": {
					"from": "m1_companies", 
					"localField": "company",
					"foreignField": "_id",
					"as": "company_info"
				}
			},
			{
				# Unwind the company_info array to get a single document for each person
				# (since $lookup returns an array of documents):
				"$unwind": "$company_info"
			},
			{
				# Project the fields we want to include in the output:
				# _id is set to 0 to exclude it from the output:
				"$project": {
					"_id": 0,
					"firstName": 1,
					"lastName": 1,
					"companyName": "$company_info.name"
				}
			}
		])
		# Measure query execution time:
		query_time = time.time() - start_time
		# Instead of printing the entire results set, as this would do:
		# print("--- %s seconds ---" % (query_time) + str(result))
		# we can limit the output to the first 10 documents:
		results = list(results)  
		for result in results[:10]: 
			print(result)
		# Print the query execution time and reult:
		print(f"--- Q1 execution time: {query_time:.4f} seconds ---")

		# Close the MongoDB connection:
		client.close()
		return
	
	# QUERY 2
	# For each company, retrieve its name and the number of employees.

	def query_q2a(self):
		client = MongoClient("mongodb://localhost:27017/")
		db = client["test"]
	
		persons = db["m1_persons"]
		# Get time at the start of the query:
		start_time = time.time()
		
		# We use again the aggregation framework to join the two collections:
		results = persons.aggregate([
			{
				# group by company and count the number of employees:
				"$group:": {
					"_id": "$company",
					"employees": {
						"$sum": 1
					}
				}
			},
			{
				# we use $lookup to join the two collections:
				# since we grouped by company, the ObjectID is now the company's ObjectId ("_id")
				"$lookup": {
					"from": "m1_companies", 
					"localField": "_id",
					"foreignField": "_id",
					"as": "company_info"
				}
			},
			{
				# we unwind the company_info array to get a single document for each company:
				"$unwind": "$company_info"
			},
			{
				# last, we project the fields we want to include in the output:
				"$project": {
					"_id": 0,
					"companyName": "$company_info.name",
					"employees": 1
				}
			}
		])
		# Measure query execution time:
		query_time = time.time() - start_time
		# results:
		results = list(results)  
		for result in results[:10]: 
			print(result)
		# Print the query execution time and reult:
		print(f"--- Q2a execution time: {query_time:.4f} seconds ---")

		# Close the MongoDB connection:
		client.close()
		return

	# We aklso present a second (improved) solution:
	# This approach also covers the case that compamnies with 0 employees are returned.
	def query_q2b(self):
		client = MongoClient("mongodb://localhost:27017/")
		db = client["test"]
	
		companies = db["m1_companies"]
		# Get time at the start of the query:
		start_time = time.time()
		
		# We use again the aggregation framework to join the two collections:
		results = companies.aggregate([
			{
				# we do a reverse lookup:
				"$lookup": {
					"from": "m1_persons",
					"localField": "_id",
					"foreignField": "company",
					"as": "employees"
				}
			},
			{
				"$project": {
					"_id": 0,
					"companyName": "$name",
					"employeeCount": { "$size": "$employees" }
				}
			}
		])
		# Measure query execution time:
		query_time = time.time() - start_time
		# results:
		results = list(results)
		for result in results[:10]: 
			print(result)
		# Print the query execution time and reult:			
		print(f"--- Q2b execution time: {query_time:.4f} seconds ---")
		# Close the MongoDB connection:
		client.close()
		return
	
	# QUERY 3
	# For each person born before 1988, update their age to “30”.

	def query_q3(self):
		client = MongoClient("mongodb://localhost:27017/")
		db = client["test"]
		persons = db["m1_persons"]
		# Get time at the start of the query:
		start_time = time.time()

		results = persons.updateMany(
			# we use the less than operator, so we update the age with "$set" 
			# for all persons that are born before 1988-01-01:
			{ "$dateOfBirth": {"$lt": "1988-01-01" } },
			{ "$set": { "age": 30 } }
		)
		# Measure query execution time: 
		query_time = time.time() - start_time

		# we show how many documents have been modfied:
		print(f"Matched {results.matched_count} documents, Modified {results.modified_count} documents.")
		print(f"--- Q3 execution time: {query_time:.4f} seconds ---")
		
		client.close()
		return

	# QUERY 4
	# For each company, update its name to include the word “Company”.

	def query_q4(self):
		client = MongoClient("mongodb://localhost:27017/")
		db = client["test"]
	
		companies = db["m1_companies"]
		# Get time at the start of the query:
		start_time = time.time()

		# here we just need the companies;
		# we will add the word "company" to its current name,
		# instead of substituting it. There are two verisons how this could be done,  
		# one, that is just appending that word to the name and another one,
		# that is checking, whether the company's name yet includes "Company".
		# We are doing the second approach:
		results = companies.update_many(
			# we need to sue not and regex, to only match companies where "Company" is not already in the name:
			{ "name": { "$not": { "$regex": "Company", "$options": "i"} } },
			[
				# we concatinate inside, which is the aggregation pipeline form of update_many:
				{ "$set": { "name": { "$concat": [ "$name", " Company" ] } } }
			]
		)

		# Measure query execution time: 
		query_time = time.time() - start_time

		# we show how many documents have been modfied:
		print(f"Matched {results.matched_count} documents, Modified {results.modified_count} documents.")
		print(f"--- Q4 execution time: {query_time:.4f} seconds ---")
		
		client.close()
		return

