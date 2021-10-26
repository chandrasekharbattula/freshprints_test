# ETL Pipeline using GCP Services
Take Home task from Fresh prints interview.

## Objective: 
<p> Build an ETL pipeline from google sheets to PostgreSQL installed on your GCP
instance </p>

  * Create a PostgreSQL instance on your own trial GCP
  * Build a pipeline using GCP services to move sample data from Google Excel sheet 
    * Create google sheets with sample data
    * Create a table with required columns to store the data on PostgreSQL 
    * Transform some of the columns from excel and clean/modify the data, Try to transform at least 4 different columns with 4 different types of data types 
      * Int 
      * String 
      * Date 
      * Decimal 
    * Create a function or pipeline using Google Cloud services to transform the extracted data and transform-load/load-transform it into the destination table on GCP PostgreSQL 
    * Create a function to append the data to GCP PostgreSQL 
    * Create a function to delete the existing data on GCP PostgreSQL using Google Cloud services 
    * Create a function to compare and update the existing data on GCP PostgreSQL using Google Cloud services 
  * Create a schedule to automate the ETL pipeline 
  * Write test cases to QA the data quality between source and destination 
  * Demonstrate it to the interviewer on call

## Solution

<p> The complete solution to this task along with the GCP services used and the source code is present in this repo </p>

<p> The GCP Services that are used to complete this task are :</p>

    * Bigquery - External Table (To access the google sheet).
    * Cloud SQL - PostgresSQL instance.
    * Security Manager - To store and access the PostgresSQL secrets.
    * Cloud Functions - To execute the individual tasks of of the ETL pipeline.
    * Cloud Composer - To schedule and orchestrate the ETL pipeline.

<p> Below are the details of folders in this repo. </p>

* /airflow - This folder contains the python code for the Airflow DAG that is used to schedule and orchestrate the ETL Pipeline. 
* /cloud_functions - This folder contains the cloud functions for individual task such as loading data to Postgres, Delete data, Appending data and modifying data.
* /docs - This folder contains the document that explains the task solutions, GCP services used with screenshots of the execution.

Note: All the instances are directly created through console. I do have practical knowledge on how we can create the instances/infra through terraform and deploy the code through gcloud commands which can be executed on Jenkins. 
