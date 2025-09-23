from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag
def user_processing():

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS users (
            id INT PRIMARY KEY,
            firstname VARCHAR(255),
            lastname VARCHAR(255),
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
        from datetime import datetime
        return {
            "id": fake_user["id"],
            "firstname": fake_user["personalInfo"]["firstName"],
            "lastname": fake_user["personalInfo"]["lastName"],
            "email": fake_user["personalInfo"]["email"],
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

    @task
    def process_user(user_info):
        import csv
        import os

        # Usar um caminho consistente
        file_path = "/tmp/user_info.csv"
        
        # Garantir que a ordem das colunas corresponda à estrutura da tabela
        fieldnames = ["id", "firstname", "lastname", "email", "created_at"]
        
        with open(file_path, mode="w", newline='') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerow(user_info)
        
        print(f"Arquivo CSV criado em: {file_path}")

    @task
    def store_user():
        hook = PostgresHook(postgres_conn_id="postgres")
        
        # Usar o mesmo caminho definido na função anterior
        file_path = "/tmp/user_info.csv"
        
        # Verificar se o arquivo existe antes de tentar copiá-lo
        import os
        if os.path.exists(file_path):
            print(f"Arquivo encontrado: {file_path}")
            hook.copy_expert(
                sql="COPY users FROM STDIN WITH CSV HEADER",
                filename=file_path
            )
        else:
            raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")

    # Definir as dependências
    create_table_task = create_table
    fake_user = is_api_available()
    user_info = extract_user(fake_user)
    process_task = process_user(user_info)
    store_task = store_user()
    
    # Estabelecer a ordem de execução
    create_table_task >> fake_user >> user_info >> process_task >> store_task
