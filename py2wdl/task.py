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

    def get_outputs(self) -> list[WDLValue]:
        return list(chain(*(task.get_outputs() for task in self.tasks)))

    def branch(self, values: list[WDLValue]) -> None:
        for task in self.tasks:
            task(*values)


class ParallelTasks(Tasks):
    def forward(self, values: list[WDLValue]) -> None:
        for task in self.tasks:
            task(*values)


class DistributedTasks(Tasks):
    def forward(self, values: list[WDLValue]) -> None:
        i = 0
        for task in self.tasks:
            length = len(task.input_types)
            task(*values[i : i + length])
            i += length


class Values(WorkflowComponent):
    def __init__(self, *values: WDLValue):
        super().__init__()
        self.values: Iterable[WDLValue] = values

    def __iter__(self) -> Iterator[WDLValue]:
        return iter(self.values)

    def get_outputs(self) -> list[WDLValue]:
        return self.values


class WDLValue:
    def __init__(
        self,
        parent_task: Optional[Task] = None,
        output_idx: Optional[int] = None,
    ) -> None:

        self.name: str = str(id(self))
        self.parent_task: Optional[Task] = parent_task
        self.output_idx: Optional[int] = output_idx
        self.children: list[tuple[Task, int]] = []
        self.wrapped: bool = False
        self.array: Union[None, Array] = None

    def wrap(self) -> Array:
        self.wrapped = True
        self.array = Array(
            element_type=type(self),
            parent_task=self.parent_task,
            output_idx=self.output_idx,
        )
        return self.array

    def is_wrapped(self) -> bool:
        return self.wrapped

    def add_child(self, child_task: Task, input_idx: int) -> None:
        self.children.append((child_task, input_idx))


class Boolean(WDLValue):
    def __init__(
        self,
        value: Optional[bool] = None,
        parent_task: Optional[Task] = None,
        output_idx: Optional[int] = None,
    ) -> None:

        super().__init__(parent_task, output_idx)
        self.value: Optional[bool] = value


class Int(WDLValue):
    def __init__(
        self,
        value: Optional[int] = None,
        parent_task: Optional[Task] = None,
        output_idx: Optional[int] = None,
    ) -> None:

        super().__init__(parent_task, output_idx)
        self.value: Optional[int] = value


class String(WDLValue):
    def __init__(
        self,
        value: Optional[str] = None,
        parent_task: Optional[Task] = None,
        output_idx: Optional[int] = None,
    ) -> None:

        super().__init__(parent_task, output_idx)
        self.value: Optional[str] = value


class File(String): ...


class Condition(String): ...


T = TypeVar("T", bound=WDLValue)


class Array(WDLValue, Generic[T]):
    def __init__(
        self,
        element_type: Type[WDLValue],
        value: list[Union[bool, int, str]] = [],
        parent_task: Optional[Task] = None,
        output_idx: Optional[int] = None,
    ) -> None:

        super().__init__(parent_task, output_idx)
        self.element_type: Type[WDLValue] = element_type
        self.value: list[Union[list, bool, int, str]] = value
        self.element: WDLValue = self.element_type(
            parent_task=parent_task, output_idx=output_idx
        )

    def get_element_type(self) -> Type[WDLValue]:
        return self.element_type


class Task(WorkflowComponent):
    def __init__(
        self,
        func: Callable[..., Any],
        name: str,
        input_types: Iterable[Type[WDLValue]] = (),
        output_types: Iterable[Type[WDLValue]] = (),
        meta: dict[str, Any] = {},
    ) -> None:

        super().__init__()
        self.func: Callable[..., Any] = func
        self.name: str = name
        self.meta: Optional[dict[str, Any]] = meta

        self.input_types: Iterable[Type[WDLValue]] = input_types
        self.outputs: list[WDLValue] = []
        if output_types is not None:
            self.setting_output_values(output_types)

        self.condition: Union[bool, None] = None
        for output in self.outputs:
            if type(output) is Condition:
                self.condition = output
                break

    def setting_output_values(self, output_types: Iterable[Type[WDLValue]]) -> None:
        for i, output_type in enumerate(output_types):
            if get_origin(output_type) is Array:
                element_type = get_args(output_type)[0]
                output = output_type(
                    parent_task=self, output_idx=i, element_type=element_type
                )
            else:
                output = output_type(parent_task=self, output_idx=i)
            self.outputs.append(output)

    def get_outputs(self) -> list[WDLValue]:
        if self.condition is not None:
            return [output for output in self.outputs if type(output) != Condition]
        else:
            return self.outputs

    def __call__(self, *args: WDLValue) -> Union[WDLValue, list[WDLValue]]:
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
                arg.add_child(self, i)

        return self.get_outputs()

    def forward(self, values: list[WDLValue]) -> None:
        self(*values)

    def scatter(self, values: list[WDLValue]) -> None:
        values = [
            value.element if isinstance(value, Array) else value for value in values
        ]
        self(*values)

    def gather(self, values: list[WDLValue]) -> None:
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
    input_types: Iterable[Type[WDLValue]] = (),
    output_types: Iterable[Type[WDLValue]] = (),
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
