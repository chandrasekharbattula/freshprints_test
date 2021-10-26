import json
import logging
import traceback

import psycopg2
from flask import abort
from google.cloud import secretmanager
from google.cloud.exceptions import GoogleCloudError

GCP_PROJECT = 'premium-bearing-329720'


def delete_data(request):
    try:
        delete_data_from_postgres()
    except Exception as e:
        msg = traceback.format_exc()
        logging.error("Function failed with error: {}".format(msg))
        return json.dumps({"status": "FAILED", "message": "Finished with error: {}".format(msg)})
    return json.dumps({"status": "SUCCESS", "message": "Successfully finished"})


def delete_data_from_postgres():
    postgres_pwd = get_secret()
    connection = None
    try:
        connection = psycopg2.connect(host="34.135.255.97",
                                      dbname="employee",
                                      user="postgres",
                                      password=f"{postgres_pwd}")

        with connection.cursor() as cursor:

            cursor.execute("""
                DELETE FROM employee_details;
            """)
            connection.commit()
            logging.info(cursor.rowcount, "Records deleted successfully into employee_details table")
    except Exception as e:
        logging.error("Error in deleting data to postgres instance", str(e))
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
