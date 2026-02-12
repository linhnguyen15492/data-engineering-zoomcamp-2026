from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bash_operator_demo",
    default_args=default_args,
    description="A simple bash operator demo DAG",
    start_date=datetime(2025, 6, 30),
    schedule=None,
    catchup=False,
    tags=["demo", "bash"],
    max_active_runs=1,  # Giới hạn chỉ 1 run active để tránh conflict
) as dag:
    # Simple command execution with proper error handling
    echo_task = BashOperator(
        task_id="echo_hello",
        bash_command='echo "Hello from BashOperator!" && exit 0',
        retries=2,
        retry_delay=timedelta(seconds=30),
    )

    # Execute a multi-line command or script
    multi_command_task = BashOperator(
        task_id="run_multiple_commands",
        bash_command="""
            set -e  # Exit on any error
            echo "This is the first line."
            sleep 2
            echo "This is the second line after a delay."
            echo "Task completed successfully"
            """,
        retries=2,
        retry_delay=timedelta(seconds=30),
    )

    # Execute an external script (assuming 'my_script.sh' exists in the DAGs folder or a specified path)
    # Note: Add a space after the script name if it ends with .sh or .bash to prevent Jinja templating.
    # Alternatively, ensure the script is not templated if not intended.
    # script_task = BashOperator(
    #     task_id='execute_external_script',
    #     bash_command='bash my_script.sh '
    # )

    # Using templating with Jinja (e.g., accessing Airflow variables)
    templated_command_task = BashOperator(
        task_id="templated_command",
        bash_command='echo "Current DAG run ID: {{ dag_run.run_id }}" && echo "Task completed successfully"',
        retries=2,
        retry_delay=timedelta(seconds=30),
    )

echo_task >> multi_command_task >> templated_command_task  # type: ignore
