
from functools import wraps
from airflow import DAG
from airflow.sdk import dag
from rollback import rollback


@wraps(dag)
def cloudio_flow(*dag_args, **dag_kwargs):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with DAG(dag_id=func.__name__, *dag_args, **dag_kwargs) as dag:
                func(*args, **kwargs)
                rollback(dag)
        return wrapper
    return decorator
