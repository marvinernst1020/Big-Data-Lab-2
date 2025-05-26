# coding=utf-8
import datetime
import time
import json
from pymongo import MongoClient
from faker import Faker

# M3: One document for “Company” with “Person” as embedded documents.

class Model3:
	def data_generator(self, n):
		# Connect to MongoDB
		client = MongoClient("mongodb://localhost:27017/")
		db = client["test"]
		# Delete collection data if exists:
		db.drop_collection("m3_companies")
		# Reference to the collection:
		companies = db["m3_companies"]
		# We again use the same faker:
		fake = Faker(['en_US'])
		# we keep the same number of companies and people as in M1 and M2:
		n_companies = int(n // 500)
		n_persons = n - n_companies
		persons_per_company = int(n_persons // n_companies)
	
		# Next we generate and insert:
		for i in range(n_companies):
			# We generate a person with all the attributes from the UML:
			# staff is a list of persons:
			staff = []
			for _ in range(persons_per_company):
				# we get fake birth year between 1950 and 2000 as in M1:
				birth_date = fake.date_of_birth(minimum_age=25, maximum_age=75)
				person = {
                    "age": 2025 - birth_date.year,
                    "companyEmail": fake.email(),
                    "dateOfBirth": birth_date.strftime("%Y-%m-%d"),
                    "firstName": fake.first_name(),
                    "lastName": fake.last_name(),
                    "sex": fake.random_element(elements=("M", "F"))
                }
				staff.append(person)
			# And now we actually generate the company:
			company = {
                "domain": fake.domain_name(),
                "email": fake.company_email(),
                "name": fake.company(),
                "url": fake.url(),
                "vatNumber": fake.vat_id(),
                "persons": staff
            }
			# Insert the document into the collection:
			companies.insert_one(company)
			print(f"{i + 1} companies inserted with {persons_per_company} persons")

		# Close the MongoDB connection:
		client.close()
		return

	# QUERY 1
	# For each person, retrieve their full name and their company’s name.

	def query_q1(self):
		client = MongoClient("mongodb://localhost:27017/")
		db = client["test"]
		companies = db["m3_companies"]
		# Get time at the start of the query:
		start_time = time.time()
		
		# We use an aggregation pipeline to unwind the persons array and project the required fields:
		results = companies.aggregate([
			{ "$unwind": "$persons" },
			{ "$project": {
				"_id": 0,
				"fullName": {
					# Concatenate first and last name of the person 
					# (is easier here this way, since we have many persons for one company):
					"$concat": ["$persons.firstName", " ", "$persons.lastName"]
				},
				"companyName": "$name"
			}}
		])
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
		companies = db["m3_companies"]
		# Get time at the start of the query:
		start_time = time.time()
		
		# We use aggregation framework to group by company name and count employees:
		# (here it is the simplest version, so we expect this to be solved the fastest)
		results = companies.aggregate([
			{ "$project": {
				"_id": 0,
				"name": 1,
				"num_employees": { "$size": "$persons"}
				}
			}
		])
		# Measure query execution time:
		query_time = time.time() - start_time
		results = list(results)  
		for result in results[:10]: 
			print(result)
		print(f"--- Q2 execution time: {query_time:.4f} seconds ---")

		# Close the MongoDB connection:
		client.close()
		return
	
	# QUERY 3
	# For each person born before 1988, update their age to “30”.

	def query_q3(self):
		client = MongoClient("mongodb://localhost:27017/")
		db = client["test"]
		companies = db["m3_companies"]
		# Getting the time started:
		start_time = time.time()

		# This query is different now from how it been done compared to the ones
		# in M1 and M2, since here the persons are embedded documents in company.
		# Thus, we need to use update_many again:
		results = companies.update_many(
			{ "persons.dateOfBirth": { "$lt": "1988-01-01" } },
			{ "$set": { "persons.$[elem].age": 30 } },
			# we need to define which array items get modified, 
			# or else, every persons age would get modified in that array:
			array_filters=[{ "elem.dateOfBirth": { "$lt": "1988-01-01" } }]
		)

		# Measure query execution time:
		query_time = time.time() - start_time
		print(f"Matched {results.matched_count} documents, Modified {results.modified_count} documents.")
		print(f"--- Q3 execution time: {query_time:.4f} seconds ---")

		# Close the MongoDB connection:
		client.close()
		return

	# QUERY 4
	# For each company, update its name to include the word “Company”.

	def query_q4(self):
		client = MongoClient("mongodb://localhost:27017/")
		db = client["test"]
	
		companies = db["m3_companies"]
		# Get time at the start of the query:
		start_time = time.time()

		# This is very similar to M1 (if not almost the same):
		results = companies.update_many(
			{ "name": { "$not": { "$regex": "Company", "$options": "i" } } },
			[
				{ "$set": { "name": { "$concat": [ "$name", " Company" ] } } }
			]
		)
		# Measure query execution time:
		query_time = time.time() - start_time
		print(f"Matched {results.matched_count} documents, Modified {results.modified_count} documents.")
		print(f"--- Q4 execution time: {query_time:.4f} seconds ---")

		# Close the MongoDB connection:
		client.close()
		return