import datetime
import time
import json
from pymongo import MongoClient
from faker import Faker

# M2: One document for “Person” with “Company” as embedded document.

class Model2:
	def data_generator(self, n):
		# Connect to MongoDB
		client = MongoClient("mongodb://localhost:27017/")
		db = client["test"]
		# Delete collection data if exists:
		db.drop_collection("m2_persons")
		# Reference to the collection (MongoDB will create it automatically 
		# - this time we did not use create_collection):
		persons = db["m2_persons"]
		# We again use the same faker:
		fake = Faker(['en_US'])
		# we keep the same number of companies and people as in M1:
		n_companies = int(n // 500)
		n_persons = n - n_companies
		# we pre-generate the companies:
		companies = []
		for _ in range(n_companies):
			company = {
				"domain": fake.domain_name(),
				"email": fake.company_email(),
				"name": fake.company(),
				"url": fake.url(),
				"vatNumber": fake.vat_id()
			}
			companies.append(company)
		# Next we generate and insert the persons and embed a company from the list:
		for i in range(n_persons):
			# we get fake birth year between 1950 and 2000 as in M1:
			birth_date = fake.date_of_birth(minimum_age=25, maximum_age=75)
			# We generate a person with all the attributes from the UML:
			person = {
				"age": 2025 - birth_date.year,
				"companyEmail": fake.email(),
				"dateOfBirth": birth_date.strftime("%Y-%m-%d"),
				"firstName": fake.first_name(),
				"lastName": fake.last_name(),
				"sex": fake.random_element(elements=("M", "F")),
				"company": companies[i % n_companies]  # embed 1 of the pre-generated companies 
				# this distributes persons evenly in round-robin over the companies using modulo arithmetic
				# so each company will have approximately the same number of employees
			}
			# Insert the document into the collection:
			persons.insert_one(person)
			print(str(i + 1) + ". Person inserted")

		# Close the MongoDB connection:
		client.close()
		return
	
	# QUERY 1
	# For each person, retrieve their full name and their company’s name.

	def query_q1(self):
		client = MongoClient("mongodb://localhost:27017/")
		db = client["test"]
		persons = db["m2_persons"]
		# Get time at the start of the query:
		start_time = time.time()
		
		# This is really simple, we can just find all documts and project the fields we want:
		results = persons.find(
			{},
			{
				"firstName": 1,
				"lastName": 1,
				"company.name": 1
			}
		)
		# Measure query execution time:
		query_time = time.time() - start_time
		# Again, we don't print all results:
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

	def query_q2(self):
		client = MongoClient("mongodb://localhost:27017/")
		db = client["test"]
		persons = db["m2_persons"]
		# Get time at the start of the query:
		start_time = time.time()
		
		# We use aggregation framework to group by company name and count employees:
		results = persons.aggregate([
			{"$group": {
				"_id": "$company.name",
				"num_employees": {"$sum": 1}
			}},
			{"$project": {
				"company_name": "$_id",
				"num_employees": 1,
				"_id": 0
			}}
		])
		# Measure query execution time:
		query_time = time.time() - start_time
		# results:
		results = list(results)  
		for result in results[:10]: 
			print(result)
		# Print the query execution time and reult:
		print(f"--- Q2 execution time: {query_time:.4f} seconds ---")

		# Close the MongoDB connection:
		client.close()
		return
	
	# QUERY 3
	# For each person born before 1988, update their age to “30”.

	def query_q3(self):
		client = MongoClient("mongodb://localhost:27017/")
		db = client["test"]
		persons = db["m2_persons"]
		# Get time at the start of the query:
		start_time = time.time()

		# here we update exactly the same as we updates in the first model:
		results = persons.update_many(
			{ "dateOfBirth": {"$lt": "1988-01-01"} },
			{ "$set": {"age": 30 } }
		)

		query_time = time.time() - start_time

		print(f"Matched {results.matched_count} documents, Modified {results.modified_count} documents.")
		print(f"--- Q3 execution time: {query_time:.4f} seconds ---")

		client.close()
		return

	# QUERY 4
	# For each company, update its name to include the word “Company”.

	def query_q4(self):
		client = MongoClient("mongodb://localhost:27017/")
		db = client["test"]
	
		persons = db["m2_persons"]
		# Get time at the start of the query:
		start_time = time.time()

		# This one we need to go into every person's document and then update the company name:
		results = persons.update_many(
			{ "company.name": { "$not": {"$regex": "Company", "$options": "i" } } },
			[
				# again we join the name of the company and " Company" together into one:
				{ "$set": { "company.name": { "$concat": [ "$company.name", " Company"] } } }
			]
		)

		query_time = time.time() - start_time

		print(f"Matched {results.matched_count} documents, Modified {results.modified_count} documents.")
		print(f"--- Q4 execution time: {query_time:.4f} seconds ---")

		client.close()
		return