from base_task import CloudioTask
from typing import ClassVar
from airflow.decorators import task


class BashCloudioTask(CloudioTask):
    @classmethod
    def get_airflow_task_decorator(cls, task_id: str):
        cwd = "{{ task.dag.folder }}"

        return task.bash(
            task_id=task_id,
            cwd=cwd
        )


class PythonCloudioTask(CloudioTask):
    requirements: ClassVar[str | list[str]] = []
   
    @classmethod
    def get_airflow_task_decorator(cls, task_id: str):
        return task.virtualenv(
            task_id=task_id,
            requirements=cls.requirements,
            system_site_packages=True,
        )
