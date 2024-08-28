from typing import Union

from .workflow import Workflow, WorkflowComponent, to_workflow
from .task import Task, Tasks
from .translator import Translator


class WorkflowManager:
    def __init__(self, indentation: str = "    ") -> None:
        self.components: set[WorkflowComponent] = set()
        self.translator: Translator = Translator(indentation=indentation)

    def add_workflow(self, workflow: Union[Workflow, WorkflowComponent]) -> None:
        if isinstance(workflow, WorkflowComponent):
            workflow = to_workflow(workflow)
        self.components.update(workflow.operands)

        base = workflow.operands[0]
        for other, operator in zip(workflow.operands[1:], workflow.operators):
            if operator == "forward":
                if base.is_scattered():
                    other.use_scatter()
                other._forward(base)

            elif operator == "branch":
                if base.is_scattered():
                    other.use_scatter()
                if not isinstance(base, Task) or not base.branching:
                    raise ValueError(
                        "Task need to return Condition for branch operation"
                    )
                other._branch(base)
            
            elif operator == "join":
                if base.is_scattered():
                    other.use_scatter()
                if not isinstance(base, Tasks):
                    raise ValueError(
                        "To perform a join operation, the left operand must be of type Tasks"
                    )
                other._join(base)

            elif operator == "scatter":
                other.use_scatter()
                other._scatter(base)

            elif operator == "gather":
                if not base.is_scattered():
                    raise ValueError(
                        f"WorkflowComponent need to be scattered for gather operation"
                    )
                other._gather(base)
            else:
                raise ValueError(f"Unsupported operator")

            base = other

    def translate(self) -> None:
        self.translator.init_wdl_script()
        for task in self.iterate_over_task():
            self.translator.generate_runnable_script(task)
            self.translator.generate_task_definition_wdl(task)
        self.translator.generate_workflow_definition_wdl(self.components)

    def iterate_over_task(self):
        tasks = set()
        for component in self.components:
            if isinstance(component, Task):
                tasks.add(component)
            elif isinstance(component, Tasks):
                tasks.update(component)
        return sorted(tasks, key=lambda x: x.name)