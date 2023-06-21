from etl_orchestrator.engine import TaskContext
def echo(ctx: TaskContext) -> None:
    print(f'[{ctx.task_id}] running')
