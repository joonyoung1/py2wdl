from task import Task
from typing import Any


class WorkflowComponent:
    def __init__(self) -> None:
        ...
    
    def execute(self) -> Any:
        ...


class Sequential(WorkflowComponent):
    def __init__(self, task: Task) -> None:
        super().__init__()
        self.task = task
    
    def execute(self, *args, **kwargs):
        return self.task.execute(*args, **kwargs)


class Conditional(WorkflowComponent):
    def __init__(self, tasks: list[Task]) -> None:
        super().__init__()
        self.tasks = tasks
    
    def execute(self, condition, *args, **kwargs):
        for task in self.tasks:
            if condition == task.name:
                return task.execute(*args, **kwargs)    


class Parallel(WorkflowComponent):
    def __init__(self) -> None:
        super().__init__()


class Scatter(WorkflowComponent):
    def __init__(self) -> None:
        super().__init__()


class Iterative(WorkflowComponent):
    def __init__(self) -> None:
        super().__init__()


class TaskNode:
    def __init__(self, task: Task) -> None:
        self.task: Task = task
        self.prev_node: list[TaskNode] = []
        self.next_nodes: list[TaskNode] = []
        self.branching: bool = False
    
    def execute(self, *args, **kwargs):
        return self.task.execute(*args, **kwargs)


if __name__ == "__main__":
    from task import task

    @task()
    def adder(a, b):
        return a + b

    print(adder)