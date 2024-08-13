import inspect
from textwrap import dedent

from .workflow import Workflow, WorkflowComponent
from .task import Task, Tasks


class WorkflowManager:
    def __init__(self) -> None:
        self.components: set[WorkflowComponent] = set()

    def add_workflow(self, workflow: Workflow) -> None:
        self.components.update(workflow.operands)

        base = workflow.operands[0]
        for other, operator in zip(workflow.operands[1:], workflow.operators):
            outputs = base.get_outputs()

            if operator == "|":
                if base.is_scattered():
                    other.use_scatter()
                other.forward(outputs)

            elif operator == "<":
                if base.is_scattered():
                    other.use_scatter()
                if not isinstance(base, Task) or base.condition is None:
                    raise ValueError("Task need to return Condition for branch operation")
                other.branch(outputs)

            elif operator == "<<":
                other.use_scatter()
                other.scatter(outputs)

            elif operator == ">>":
                if not base.is_scattered():
                    raise ValueError(f"WorkflowComponent need to be scattered for gather operation")
                other.gather(outputs)
            else:
                raise ValueError(f"Unsupported operator")
                
            base = other
    
    def translate(self) -> None:
        root_components = []
        for task in self.iterate_over_task():
            self.create_runnable_script(task)
    
    def iterate_over_task(self):
        for component in self.components:
            if isinstance(component, Task):
                yield component
            elif isinstance(component, Tasks):
                yield from component
    
    def create_runnable_script(self, task: Task) -> None:
        func_source = self.get_func_source(task)
    
    def get_func_source(self, task: Task) -> str:
        func_source = inspect.getsourcelines(task.func)[0]

        for i, line in enumerate(func_source):
            if line.strip().startswith("def "):
                return dedent(''.join(func_source[i:])).strip()
        
        raise ValueError("Function definition not found in source lines.")
