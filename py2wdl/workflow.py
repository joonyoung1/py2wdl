from __future__ import annotations
from typing import Union


def to_workflow(other: Union[WorkflowComponent, Workflow]):
    return other if isinstance(other, Workflow) else Workflow(other)


class Workflow:
    def __init__(self, operand: WorkflowComponent) -> None:
        self.operands: list[WorkflowComponent] = [operand]
        self.operators: list[str] = []

    def forward(self, other: Union[WorkflowComponent, Workflow]) -> Workflow:
        self.connect(to_workflow(other), operator="forward")
        return self

    def branch(self, other: Union[WorkflowComponent, Workflow]) -> Workflow:
        self.connect(to_workflow(other), operator="branch")
        return self
    
    def join(self, other: Union[WorkflowComponent, Workflow]) -> Workflow:
        self.connect(to_workflow(other), operator="join")
        return self

    def scatter(self, other: Union[WorkflowComponent, Workflow]) -> Workflow:
        self.connect(to_workflow(other), operator="scatter")
        return self

    def gather(self, other: Union[WorkflowComponent, Workflow]) -> Workflow:
        self.connect(to_workflow(other), operator="gather")
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
    
    def create_output_dependencies(self): ...

    def forward(self, other: Union[WorkflowComponent, Workflow]):
        return Workflow(self).forward(to_workflow(other))

    def branch(self, other: Union[WorkflowComponent, Workflow]):
        return Workflow(self).branch(to_workflow(other))

    def join(self, other: Union[WorkflowComponent, Workflow]):
        return Workflow(self).join(to_workflow(other))
    
    def scatter(self, other: Union[WorkflowComponent, Workflow]):
        return Workflow(self).scatter(to_workflow(other))
    
    def gatter(self, other: Union[WorkflowComponent, Workflow]):
        return Workflow(self).gather(to_workflow(other))