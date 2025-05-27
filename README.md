# Big Data Lab 2 – Document Stores

This repository contains the implementation of Lab 2 for the course **23D020: Big Data Management for Data Science**.

## Overview

The lab focuses on hands-on experience with MongoDB, one of the most widely used NoSQL document stores. The goal is to design, implement, and compare different data modeling strategies using Python and MongoDB.

We explore three different document modeling approaches:

- **Model 1 (Normalized):** Persons and companies stored in separate collections, with references.
- **Model 2 (Person-embedded):** Company information embedded within each person document.
- **Model 3 (Company-embedded):** Persons embedded as a list within each company document.

##Folder Contents

- `model1.py`, `model2.py`, `model3.py`: Core scripts implementing each model with data generation and queries.
- `run_all.py`: Main script to generate data and execute all queries for all models. It supports a user input for document count and can optionally suppress detailed output.
- `upcschool_mongolab.py`: An interactive CLI to test each model individually.
- `README.md`: This file.

## How to Run

1. Install the required packages (e.g. with `pip install -r requirements.txt`).
2. Make sure MongoDB is running locally at `localhost:27017`.
3. Run the script:

```bash
python run_all.py
```
