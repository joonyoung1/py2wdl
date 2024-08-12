from __future__ import annotations
from typing import Union


def to_workflow(other: Union[WorkflowComponent, Workflow]):
    return other if isinstance(other, Workflow) else Workflow(other)


class Workflow:
    def __init__(self, operand: WorkflowComponent) -> None:
        self.operands: list[WorkflowComponent] = [operand]
        self.operators: list[str] = []

    def __or__(self, other: Union[WorkflowComponent, Workflow]) -> Workflow:
        self.connect(to_workflow(other), operator="|")

    def __lt__(self, other: Union[WorkflowComponent, Workflow]) -> Workflow:
        self.connect(to_workflow(other), operator="<")
        return self

    def __lshift__(self, other: Union[WorkflowComponent, Workflow]) -> Workflow:
        self.connect(to_workflow(other), operator="<<")
        return self

    def __rshift__(self, other: Union[WorkflowComponent, Workflow]) -> Workflow:
        self.connect(to_workflow(other), operator=">>")
        return self

    def connect(
        self,
        other: Workflow,
        operator: str,
    ) -> None:
        self.operands += other.operands
        self.operators += [operator] + other.operators


class WorkflowComponent:
    def __init__(self):
        self.scattered: bool = False
    
    def is_scattered(self) -> bool:
        return self.scattered

    def use_scatter(self) -> None:
        self.scattered = True

    def __or__(self, other: Union[WorkflowComponent, Workflow]):
        return Workflow(self) | to_workflow(other)

    def __lt__(self, other: Union[WorkflowComponent, Workflow]):
        return Workflow(self) < to_workflow(other)
    
    def __lshift__(self, other: Union[WorkflowComponent, Workflow]):
        return Workflow(self) << to_workflow(other)
    
    def __rshift__(self, other: Union[WorkflowComponent, Workflow]):
        return Workflow(self) >> to_workflow(other)