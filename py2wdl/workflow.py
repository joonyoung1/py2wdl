from __future__ import annotations
from typing import Union


def to_workflow(other: Union[WorkflowComponent, Workflow]):
    return other if isinstance(other, Workflow) else Workflow(other)


class Workflow:
    def __init__(self, component: WorkflowComponent) -> None:
        self.components: list = [component]

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
        self.components = self.components + [operator] + other.components


class WorkflowComponent:
    def __or__(self, other: Union[WorkflowComponent, Workflow]):
        return Workflow(self) | to_workflow(other)

    def __lt__(self, other: Union[WorkflowComponent, Workflow]):
        return Workflow(self) < to_workflow(other)
    
    def __lshift__(self, other: Union[WorkflowComponent, Workflow]):
        return Workflow(self) << to_workflow(other)
    
    def __rshift__(self, other: Union[WorkflowComponent, Workflow]):
        return Workflow(self) >> to_workflow(other)