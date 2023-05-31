"""Quickstart example."""
from etl_orchestrator import WorkflowEngine
from etl_orchestrator.dag import Task, Workflow

wf = Workflow(workflow_id="demo")
wf.add_task(Task(task_id="hello"))
engine = WorkflowEngine(workflow=wf, tasks={"hello": lambda c: print("hi")})
engine.run()
