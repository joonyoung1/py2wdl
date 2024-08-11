from .workflow import Workflow
from .task import Task


class WorkflowManager:
    def __init__(self) -> None:
        self.root = None

    def add_workflow(self, workflow: Workflow) -> None:
        base = workflow.operands[0]
        for other, operator in zip(workflow.operands[1:], workflow.operators):
            outputs = base.get_outputs()

            if operator == "|":
                other.forward(outputs)

            elif operator == "<":
                if not isinstance(base, Task) or base.condition is None:
                    raise ValueError("Task need to return Condition for branch operation")
                other.branch(outputs)

            elif operator == "<<":
                other.scatter(outputs)

            elif operator == ">>":
                other.gather(outputs)
            else:
                raise ValueError(f"Unsupported operator")
                
            base = other
