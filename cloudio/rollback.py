
from airflow import DAG
from airflow.sdk import task, get_current_context, TaskGroup
from airflow.utils.state import State
from airflow.models import DagRun
from airflow.utils.trigger_rule import TriggerRule
from base_task import CloudioTask


@task.branch(trigger_rule=TriggerRule.ALL_DONE)
def rollback_controller():
    context = get_current_context()
    dag_run: DagRun = context["dag_run"]
    ti = context["ti"]

    task_states = ti.get_task_states(dag_run.dag_id, run_ids=[dag_run.run_id])
    rollbacks = []
    at_least_one_failed = False

    for task_id, state in task_states[dag_run.run_id].items():
        if state == State.FAILED:
            at_least_one_failed = True

        if state in {State.SUCCESS, State.FAILED}:
            rollbacks.append(f"rollback.{task_id[:-len('_run')]}_rollback")

    if not at_least_one_failed:
        return []
    
    return rollbacks


def rollback(dag: DAG):
    rollbacks = []
    dag_leaves = dag.leaves
    with TaskGroup(group_id="rollback") as tg:
        for task in dag.tasks:
            base_cls_name = task.task_id[:-len("_run")]
            cls = CloudioTask.tasks_registry.get(base_cls_name)
            if not cls:
                raise ValueError(f"Unable to locate task {task.task_id} class for rollback: {cls}")
            rollback_method = getattr(cls, "rollback")

            rollbacks.append(
                rollback_method(
                    f'{{{{ ti.xcom_pull(task_ids="{task.task_id}") }}}}',
                    *task.op_args,
                    **task.op_kwargs 
                )
            )
        rollback_branch = rollback_controller()
        rollback_branch >> rollbacks
            
        for leaf in dag_leaves:
            leaf >> tg
