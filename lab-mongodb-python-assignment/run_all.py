"""
run_all.py

This script runs the full pipeline for Lab 2:
- Generates data for all three MongoDB models (M1, M2, M3)
- Executes queries Q1–Q4 for each model
- Prints execution time and sample outputs

To use:
1. Run this file: python run_all.py
2. Enter the number of documents you want (total = persons + companies)
"""

# we import all the models:
from model1 import Model1
from model2 import Model2
from model3 import Model3

# this function will be used after the total number of documts that should be created was inserted:
def run_all_models(n_docs):
    print("=== MODEL 1 ===")
    m1 = Model1()
    m1.data_generator(n_docs, show_only_timing)
    m1.query_q1(show_only_timing)
    m1.query_q2(show_only_timing)
    m1.query_q3(show_only_timing)
    m1.query_q4(show_only_timing)

    print("\n=== MODEL 2 ===")
    m2 = Model2()
    m2.data_generator(n_docs, show_only_timing)
    m2.query_q1(show_only_timing)
    m2.query_q2(show_only_timing)
    m2.query_q3(show_only_timing)
    m2.query_q4(show_only_timing)

    print("\n=== MODEL 3 ===")
    m3 = Model3()
    m3.data_generator(n_docs, show_only_timing)
    m3.query_q1(show_only_timing)
    m3.query_q2(show_only_timing)
    m3.query_q3(show_only_timing)
    m3.query_q4(show_only_timing)

if __name__ == "__main__":
    show_only_timing = input("Show only execution times (y/n)? ").lower().startswith("y")
    n = int(input("How many total documents (persons + companies >= 500) do you want to generate? "))
    run_all_models(n)