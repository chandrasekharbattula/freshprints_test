import json
import logging
import traceback

import google.auth
import psycopg2
from flask import abort
from google.cloud import bigquery, secretmanager
from google.cloud.exceptions import GoogleCloudError

GCP_PROJECT = 'premium-bearing-329720'


def google_sheet_data_to_postgres(request):
    try:
        employee_table_data = get_google_sheet_data()
        insert_data_to_postgres(employee_table_data)
    except Exception as e:
        msg = traceback.format_exc()
        logging.error("Function failed with error: {}".format(msg))
        return json.dumps({"status": "FAILED", "message": "Finished with error: {}".format(msg)})
    return json.dumps({"status": "SUCCESS", "message": "Successfully finished"})


def get_google_sheet_data():
    credentials, project = google.auth.default(
        scopes=["https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/bigquery", ]
    )
    client = bigquery.Client(credentials=credentials, project=project)
    table_name = 'premium-bearing-329720.employee.employee_details'
    delete_query = "SELECT employee_id, employee_first_name, employee_last_name, " \
                   "employee_doj, employee_annual_salary, employee_age, years_left_for_retirement, " \
                   f"employee_salary_currency FROM `{table_name}` "
    table = client.get_table(table_name)

    query_response = client.query(query=delete_query)

    employees_data_as_json = []

    if query_response.done():
        try:
            query_response.result()
            for row in list(query_response):
                if row.years_left_for_retirement > 0:
                    if row.employee_first_name is None or row.employee_last_name is None or \
                            row.employee_doj is None or row.employee_annual_salary is None or \
                            row.employee_age is None or row.employee_salary_currency is None:
                        logging.info(f"Skipping employee details holding the id {row.employee_id} due to invalid data")
                    else:
                        employee_data = {
                            "employee_id": row.employee_id,
                            "employee_first_name": row.employee_first_name,
                            "employee_last_name": row.employee_last_name,
                            "employee_doj": row.employee_doj,
                            "employee_salary": row.employee_annual_salary,
                            "employee_age": row.employee_age,
                            "employee_salary_currency": row.employee_salary_currency
                        }
                        employees_data_as_json.append(employee_data)
        except GoogleCloudError as e:
            logging.error("Error in querying the data from table ", str(e))
            return abort(500, json.dumps({"status": "Failed", "message": "Finished with error. Check Logs for more "
                                                                         "details"}))
        else:
            logging.info("DONE! Read {} rows from the table {}".format(table.num_rows, table_name))
            return employees_data_as_json
    else:
        logging.info("Error in querying the table data")


def insert_data_to_postgres(employee_table_data):
    postgres_pwd = get_secret()
    connection = None
    try:
        connection = psycopg2.connect(host="34.135.255.97",
                                      dbname="employee",
                                      user="postgres",
                                      password=f"{postgres_pwd}")

        with connection.cursor() as cursor:

            cursor.executemany("""
                INSERT INTO employee_details VALUES (
                    %(employee_id)s,
                    %(employee_first_name)s,
                    %(employee_last_name)s,
                    %(employee_doj)s,
                    %(employee_salary)s,
                    %(employee_age)s,
                    %(employee_salary_currency)s
                );
            """, employee_table_data)
            connection.commit()
            logging.info(cursor.rowcount, "Records inserted successfully into employee_details table")
    except Exception as e:
        logging.error("Error in inserting data to postgres instance", str(e))
        return abort(500, json.dumps({"status": "Failed", "message": "Finished with error. Check Logs for more "
                                                                     "details"}))
    finally:
        if connection:
            cursor.close()
            connection.close()
            logging.info("PostgresSQL connection is closed")

def get_secret():
    client = secretmanager.SecretManagerServiceClient()
    name = "projects/1030030299/secrets/postgres_pwd/versions/1"
    try:
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except GoogleCloudError as e:
        logging.error("Error in getting the secret value", str(e))
        return abort(500, json.dumps({"status": "Failed", "message": "Finished with error. Check Logs for more "
                                                                     "details"}))
