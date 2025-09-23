from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue

@dag
def user_processing():

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS users (
            id INT PRIMARY KEY,
            fisrtname VARCHAR(255),
            lastname VARCHAR(255),
            name VARCHAR(255),
            email VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    @task.sensor(poke_interval=30, timeout=300)
    def is_api_available():
        import requests
        response = requests.get("https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json")
        print(response.status_code)

        if response.status_code == 200:
            condition = True
            fake_user = response.json()
        else:
            condition = False
            fake_user = None
        return PokeReturnValue(is_done=condition, xcom_value=fake_user)

    @task
    def extract_user(fake_user):
        return {
            "id": fake_user["id"],
            "firstname": fake_user["personalInfo"]["firstName"],
            "lastname": fake_user["personalInfo"]["lastName"],
            "email": fake_user["personalInfo"]["email"],
        }

    @task
    def process_user(user_info):
        # store info into a csv file called user_info.csv
        import csv
        import os

        base_path = os.getcwd()
        print(f"Current working directory: {base_path}")
        # ensure the directory exists

        path = f"{base_path}/tmp"        
        os.makedirs(path, exist_ok=True)

        with open(f"{path}/user_info.csv", mode="w", newline='') as file:
            writer = csv.writer(file)
            writer.writerow([user_info["id"], user_info["firstname"], user_info["lastname"], user_info["email"]])

    fake_user = is_api_available()
    user_info = extract_user(fake_user)
    process_user(user_info)

user_processing()