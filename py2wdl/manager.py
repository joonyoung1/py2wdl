from .workflow import Workflow
from .task import Task


class WorkflowManager:
    def __init__(self) -> None:
        self.root = None

    def add_workflow(self, workflow: Workflow) -> None:
        base = workflow.components[0]
        for i in range(1, len(workflow.components), 2):
            operator = workflow.components[i]
            other = workflow.component[i + 1]

            if operator == "|":
                base.forward(other)
            elif operator == "<":
                base.branch(other)
            elif operator == "<<":
                base.scatter(other)
            elif operator == ">>":
                base.gather(other)
            else:
                raise ValueError(f"Unsupported operator")
                
            base = other
