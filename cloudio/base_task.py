from typing import Any, Callable, ClassVar
from airflow.decorators.base import TaskDecorator
from abc import ABCMeta, abstractmethod


class CloudioTaskMeta(ABCMeta):
    def __getattribute__(cls, name) -> Callable:
        returned_attribute = super().__getattribute__(name)
        get_task_id = super().__getattribute__("get_task_id")
        airflow_task_decorator = super().__getattribute__("get_airflow_task_decorator")

        
        if name == "run" or name == "rollback":
            returned_attribute = airflow_task_decorator(get_task_id(name))(returned_attribute)
        
        return returned_attribute


class CloudioTask(metaclass=CloudioTaskMeta):
    tasks_registry: ClassVar[dict[str, "CloudioTask"]] = {}

    def __init_subclass__(cls, *args, **kwargs):
        CloudioTask.tasks_registry[cls.__name__] = cls
        super().__init_subclass__(*args, **kwargs)

    @classmethod
    @abstractmethod
    def get_airflow_task_decorator(cls, task_id: str) -> TaskDecorator:
        pass

    @classmethod
    def get_task_id(cls, name: str) -> str:
        """Return the unique identifier for the task."""
        return f"{cls.__name__}_{name}"
    
    @staticmethod
    @abstractmethod
    def rollback(run_output: Any, *run_args, **run_kwargs) -> Any:
        """Rollback the task with the given keyword arguments."""
        pass

    @staticmethod
    @abstractmethod
    def run(*args, **kwargs) -> Any:
        """Run the task with the given keyword arguments."""
        pass
