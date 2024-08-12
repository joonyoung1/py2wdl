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
