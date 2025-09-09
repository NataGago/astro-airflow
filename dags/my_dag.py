from airflow.sdk import dag, task
from datetime import datetime

@dag(schedule="@daily", start_date=datetime(2025, 1, 1), catchup=False, default_args={"owner": "Natã Gago", "retries": 3}, tags=["example"])
def my_dag():

    @task
    def training_model(accuracy: int):
        return accuracy
    
    # @task
    # def training_model_a():
    #     return 1

    # @task
    # def training_model_b():
    #     return 2

    # @task
    # def training_model_c():
    #     return 3
    
    @task.branch
    def choose_best_model(accuracies: list[int]):
        if max(accuracies) > 2:
            return "accurate"
        return "inaccurate"
    
    @task.bash
    def accurate():
        return "echo 'accurate'"

    @task.bash
    def inaccurate():
        return "echo 'inaccurate'"

    # accuracies = [training_model_a(), training_model_b(), training_model_c()]
    accuracies = training_model.expand(accuracy=[1, 2, 3])
    choose_best_model(accuracies) >> [accurate(), inaccurate()]

my_dag()