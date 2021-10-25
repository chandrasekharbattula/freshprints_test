# freshprints_test
Take Home task from Fresh prints interview.

Objective: Build an ETL pipeline from google sheets to PostgreSQL installed on your GCP
instance
● Create a PostgreSQL instance on your own trial GCP
● Build a pipeline using GCP services to move sample data from Google Excel sheet
○ Create google sheets with sample data
○ Create a table with required columns to store the data on PostgreSQL
○ Transform some of the columns from excel and clean/modify the data, Try to
transform at least 4 different columns with 4 different types of data types
  ■ Int
  ■ String
  ■ Date
  ■ Decimal
○ Create a function or pipeline using Google Cloud services to transform the
extracted data and transform-load/load-transform it into the destination table on
GCP PostgreSQL
○ Create a function to append the data to GCP PostgreSQL
○ Create a function to delete the existing data on GCP PostgreSQL using Google
Cloud services
○ Create a function to compare and update the existing data on GCP PostgreSQL
using Google Cloud services
● Create a schedule to automate the ETL pipeline
● Write test cases to QA the data quality between source and destination
● Demonstrate it to the interviewer on call
