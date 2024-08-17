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
            outputs = base.get_outputs()

            if operator == "|":
                if base.is_scattered():
                    other.use_scatter()
                other.forward(outputs)

            elif operator == "<":
                if base.is_scattered():
                    other.use_scatter()
                if not isinstance(base, Task) or base.condition is None:
                    raise ValueError(
                        "Task need to return Condition for branch operation"
                    )
                other.branch(outputs)

            elif operator == "<<":
                other.use_scatter()
                other.scatter(outputs)

            elif operator == ">>":
                if not base.is_scattered():
                    raise ValueError(
                        f"WorkflowComponent need to be scattered for gather operation"
                    )
                other.gather(outputs)
            else:
                raise ValueError(f"Unsupported operator")

            base = other

    def translate(self) -> None:
        for task in self.iterate_over_task():
            self.translator.generate_runnable_script(task)
            self.translator.generate_task_definition_wdl(task)

    def iterate_over_task(self):
        for component in self.components:
            if isinstance(component, Task):
                yield component
            elif isinstance(component, Tasks):
                yield from component