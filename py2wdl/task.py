from __future__ import annotations
import inspect
from textwrap import dedent
from itertools import chain

from typing import Optional, Callable, Iterable, Iterator, Union, Type, Any
from typing import TypeVar, Generic
from typing import get_origin, get_args

from .workflow import WorkflowComponent


class Tasks(WorkflowComponent):
    def __init__(self, *tasks: Task):
        super().__init__()
        self.tasks: Iterable[Task] = tasks

    def __iter__(self) -> Iterator[Task]:
        return iter(self.tasks)

    def create_output_dependencies(self) -> list[Dependency]:
        return list(chain(*(task.create_output_dependencies() for task in self.tasks)))

    def branch(self, values: list[Dependency]) -> None:
        for task in self.tasks:
            task(*values)


class ParallelTasks(Tasks):
    def forward(self, other: WorkflowComponent) -> None:
        for task in self.tasks:
            task(*other.create_output_dependencies())


class DistributedTasks(Tasks):
    def forward(self, other: WorkflowComponent) -> None:
        values = other.create_output_dependencies()
        i = 0
        for task in self.tasks:
            length = len(task.input_types)
            task(*values[i : i + length])
            i += length


class Values(WorkflowComponent):
    def __init__(self, *values: Dependency):
        super().__init__()
        self.values: Iterable[Dependency] = values

    def __iter__(self) -> Iterator[Dependency]:
        return iter(self.values)

    def create_output_dependencies(self) -> list[Dependency]:
        return self.values


class Dependency:
    def __init__(
        self,
        parent_task: Optional[Task] = None,
        output_idx: Optional[int] = None,
        child_task: Optional[Task] = None,
        input_idx: Optional[int] = None,
    ):
        
        self.parent_task: Optional[Task] = parent_task
        self.output_idx: Optional[int] = output_idx
        self.child_task: Optional[Task] = child_task
        self.input_idx: Optional[int] = input_idx

        self.wrapped: bool = False
        self.array: Union[None, Array] = None

    def set_child(self, child_task: Task, input_idx: int) -> None:
        self.child_task = child_task
        self.input_idx = input_idx

    # def wrap(self) -> Array:
    #     self.wrapped = True
    #     self.array = Array(
    #         element_type=type(self),
    #         parent_task=self.parent_task,
    #         output_idx=self.output_idx,
    #     )
    #     return self.array

    # def is_wrapped(self) -> bool:
    #     return self.wrapped


class Boolean(Dependency):
    def __init__(
        self,
        value: Optional[bool] = None,
        parent_task: Optional[Task] = None,
        output_idx: Optional[int] = None,
    ) -> None:

        super().__init__(parent_task, output_idx)
        self.value: Optional[bool] = value

    @classmethod
    def repr(cls):
        return "Boolean"


class Int(Dependency):
    def __init__(
        self,
        value: Optional[int] = None,
        parent_task: Optional[Task] = None,
        output_idx: Optional[int] = None,
    ) -> None:

        super().__init__(parent_task, output_idx)
        self.value: Optional[int] = value

    @classmethod
    def repr(cls):
        return "Int"


class Float(Dependency):
    def __init__(
        self,
        value: Optional[float] = None,
        parent_task: Optional[Task] = None,
        output_idx: Optional[int] = None,
    ) -> None:

        super().__init__(parent_task, output_idx)
        self.value: Optional[float] = value

    @classmethod
    def repr(cls):
        return "Float"


class String(Dependency):
    def __init__(
        self,
        value: Optional[str] = None,
        parent_task: Optional[Task] = None,
        output_idx: Optional[int] = None,
    ) -> None:

        super().__init__(parent_task, output_idx)
        self.value: Optional[str] = value

    @classmethod
    def repr(cls):
        return "String"


class File(String):
    @classmethod
    def repr(cls):
        return "File"


class Condition(String): ...


T = TypeVar("T", bound=Dependency)


class Array(Dependency, Generic[T]):
    def __init__(
        self,
        element_type: Type[Dependency],
        value: list[Union[bool, int, str]] = [],
        parent_task: Optional[Task] = None,
        output_idx: Optional[int] = None,
    ) -> None:

        super().__init__(parent_task, output_idx)
        self.element_type: Type[Dependency] = element_type
        self.value: list[Union[list, bool, int, str]] = value
        self.element: Dependency = self.element_type(
            parent_task=parent_task, output_idx=output_idx
        )

    def get_element_type(self) -> Type[Dependency]:
        return self.element_type


class Task(WorkflowComponent):
    def __init__(
        self,
        func: Callable[..., Any],
        name: str,
        input_types: Iterable[Type[Dependency]] = (),
        output_types: Iterable[Type[Dependency]] = (),
        meta: dict[str, Any] = {},
    ) -> None:

        super().__init__()
        self.func: Callable[..., Any] = func
        self.name: str = name
        self.meta: Optional[dict[str, Any]] = meta

        self.input_types: Iterable[Type[Dependency]] = input_types
        self.output_types: Iterable[Type[Dependency]] = output_types

        self.inputs: list[list[Dependency]] = [[] for _ in range(len(self.input_types))]
        self.outputs: list[list[Dependency]] = [[] for _ in range(len(self.output_types))]

        self.condition: Union[Dependency, None] = None
        for output in self.outputs:
            if type(output) is Condition:
                self.condition = output
                break

    def create_output_dependencies(self) -> list[Dependency]:
        outputs = []
        for i, output_type in enumerate(self.output_types):
            if output_type is Condition:
                continue
            elif get_origin(output_type) is Array:
                element_type = get_args(output_type)[0]
                output = output_type(
                    parent_task=self, output_idx=i, element_type=element_type
                )
            else:
                output = output_type(parent_task=self, output_idx=i)
            outputs.append(output)
            self.outputs[i].append(output)
        
        return outputs

    def __call__(self, *args: Dependency) -> Union[Dependency, list[Dependency]]:
        if len(args) != len(self.input_types):
            raise TypeError(
                f"Expected {len(self.input_types)} arguments but got {len(args)}"
            )

        for i, (arg, t) in enumerate(zip(args, self.input_types)):
            origin = get_origin(t)
            if (origin is None and not isinstance(arg, t)) or (
                origin is Array
                and not (isinstance(arg, Array) and arg.element_type is get_args(t)[0])
            ):
                raise TypeError(
                    f"Expected type {t} on argument {i}, but got {type(arg)}"
                )
            else:
                arg.set_child(self, i)
                self.inputs[i].append(arg)

        return self.create_output_dependencies()

    def forward(self, other: WorkflowComponent) -> None:
        self(*other.create_output_dependencies())

    def scatter(self, values: list[Dependency]) -> None:
        values = [
            value.element if isinstance(value, Array) else value for value in values
        ]
        self(*values)

    def gather(self, values: list[Dependency]) -> None:
        values = [value.wrap() for value in values]
        self(*values)

    def execute(self, *args: Any, **kwargs: Any) -> Any:
        return self.func(*args, **kwargs)

    def __str__(self) -> str:
        if self.meta:
            meta_str = ", ".join(f"{k}={v!r}" for k, v in self.meta.items())
        func_source = dedent(inspect.getsource(self.func)).strip()

        return (
            f"Name: {self.name}\n"
            + (f"Metadata: ({meta_str})\n" if self.meta else "")
            + f"Function Source:\n{func_source}"
        )


def task(
    name: Optional[str] = None,
    input_types: Iterable[Type[Dependency]] = (),
    output_types: Iterable[Type[Dependency]] = (),
    meta: dict[str, Any] = {},
) -> Callable[..., Any]:

    def task_factory(func: Callable[..., Any]) -> Task:
        return Task(
            func=func,
            name=name if name else func.__name__,
            input_types=input_types,
            output_types=output_types,
            meta=meta,
        )

    return task_factory
