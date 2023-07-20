import random
from etl_orchestrator.engine import TaskContext
def flaky(ctx: TaskContext):
    if random.random() < 0.3:
        raise RuntimeError('flaky')
