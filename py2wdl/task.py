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

    def branch(self, other: WorkflowComponent) -> None:
        for task in self.tasks:
            task.connect(*other.create_output_dependencies())


class ParallelTasks(Tasks):
    def forward(self, other: WorkflowComponent) -> None:
        for task in self.tasks:
            task.connect(*other.create_output_dependencies())


class DistributedTasks(Tasks):
    def forward(self, other: WorkflowComponent) -> None:
        values = other.create_output_dependencies()
        i = 0
        for task in self.tasks:
            length = len(task.input_types)
            task.connect(*values[i : i + length])
            i += length


class Values(WorkflowComponent):
    count = 0

    def __init__(self, *deps: Dependency):
        super().__init__()
        self.values: Iterable[Dependency] = deps
        self.output_types: Iterable[Type[Dependency]] = [
            type(dep) if not isinstance(dep, Array) else Array[dep.element_type]
            for dep in deps
        ]
        self.outputs: list[list[Dependency]] = [
            [] for _ in range(len(self.output_types))
        ]

        self.lv = 1
        self.name = f"Values{Values.count}"
        Values.count += 1

    def __iter__(self) -> Iterator[Dependency]:
        return iter(self.values)

    def create_output_dependencies(self) -> list[Dependency]:
        outputs = []
        for i, output_type in enumerate(self.output_types):
            if get_origin(output_type) is Array:
                element_type = get_args(output_type)[0]
                output = output_type(
                    parent=self, output_idx=i, element_type=element_type
                )
            else:
                output = output_type(parent=self, output_idx=i)
            outputs.append(output)
            self.outputs[i].append(output)
        
        return outputs


class Dependency:
    def __init__(
        self,
        parent: Optional[WorkflowComponent] = None,
        output_idx: Optional[int] = None,
        child: Optional[Task] = None,
        input_idx: Optional[int] = None,
    ):

        self.parent: Optional[WorkflowComponent] = parent
        self.output_idx: Optional[int] = output_idx
        self.child: Optional[Task] = child
        self.input_idx: Optional[int] = input_idx

        self.wrapped: bool = False
        self.scattered: bool = False

    def set_parent(self, parent: WorkflowComponent, output_idx: int) -> None:
        self.parent = parent
        self.output_idx = output_idx

    def set_child(self, child: Task, input_idx: int) -> None:
        self.child = child
        self.input_idx = input_idx

    def wrap(self) -> Array:
        self.wrapped = True
        return self

    def scatter(self) -> Dependency:
        self.scattered = True
        return self

    def is_wrapped(self) -> bool:
        return self.wrapped

    def is_scattered(self) -> bool:
        return self.scattered


class Boolean(Dependency):
    def __init__(
        self,
        value: Optional[bool] = None,
        parent: Optional[WorkflowComponent] = None,
        output_idx: Optional[int] = None,
    ) -> None:

        super().__init__(parent, output_idx)
        self.value: Optional[bool] = value
    
    def repr(self) -> str:
        return "true" if self.value else "false"


class Int(Dependency):
    def __init__(
        self,
        value: Optional[int] = None,
        parent: Optional[WorkflowComponent] = None,
        output_idx: Optional[int] = None,
    ) -> None:

        super().__init__(parent, output_idx)
        self.value: Optional[int] = value
    
    def repr(self) -> str:
        return str(self.value)


class Float(Dependency):
    def __init__(
        self,
        value: Optional[float] = None,
        parent: Optional[WorkflowComponent] = None,
        output_idx: Optional[int] = None,
    ) -> None:

        super().__init__(parent, output_idx)
        self.value: Optional[float] = value
    
    def repr(self) -> str:
        return str(self.value)


class String(Dependency):
    def __init__(
        self,
        value: Optional[str] = None,
        parent: Optional[WorkflowComponent] = None,
        output_idx: Optional[int] = None,
    ) -> None:

        super().__init__(parent, output_idx)
        self.value: Optional[str] = value
    
    def repr(self) -> str:
        return self.value


class File(String): ...


class Condition(String): ...


T = TypeVar("T", bound=Dependency)


class Array(Dependency, Generic[T]):
    def __init__(
        self,
        element_type: Type[Dependency],
        value: list[Union[bool, int, str]] = [],
        parent: Optional[WorkflowComponent] = None,
        output_idx: Optional[int] = None,
    ) -> None:

        super().__init__(parent, output_idx)
        self.element_type: Type[Dependency] = element_type
        self.value: list[Union[list, bool, int, str]] = value
        self.element: Dependency = self.element_type(
            parent=parent, output_idx=output_idx
        )

    def get_element_type(self) -> Type[Dependency]:
        return self.element_type

    def repr(self) -> str:
        return "[" + ", ".join(self.value) +  "]"


def format_type_hint(type_hint):
    origin = get_origin(type_hint)
    args = get_args(type_hint)

    if origin is None:
        return type_hint.__name__
    else:
        formatted_args = ", ".join(arg.__name__ for arg in args)
        return f"{origin.__name__}[{formatted_args}]"


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
        self.outputs: list[list[Dependency]] = [
            [] for _ in range(len(self.output_types))
        ]
        self.branching: bool = Condition in output_types
        self.cond_idx: int = -1
        for i, output_type in enumerate(output_types):
            if output_type == Condition:
                self.cond_idx = i
                break

        self.call_script: str = ""
        self.lv: int = -1

    def create_output_dependencies(self) -> list[Dependency]:
        outputs = []
        for i, output_type in enumerate(self.output_types):
            if output_type is Condition:
                continue
            elif get_origin(output_type) is Array:
                element_type = get_args(output_type)[0]
                output = output_type(
                    parent=self, output_idx=i, element_type=element_type
                )
            else:
                output = output_type(parent=self, output_idx=i)
            outputs.append(output)
            self.outputs[i].append(output)

        return outputs

    def connect(self, *args: Dependency) -> None:
        if len(args) != len(self.input_types):
            raise TypeError(
                f"Expected {len(self.input_types)} arguments but got {len(args)}"
            )

        for i, (arg, t) in enumerate(zip(args, self.input_types)):
            origin = get_origin(t)
            is_array = isinstance(arg, Array)

            if origin is None:
                valid_arg = isinstance(arg, t) or (
                    is_array and arg.element_type is t and arg.is_scattered()
                )
            elif origin is Array:
                valid_arg = (is_array and arg.element_type is get_args(t)[0]) or (
                    not is_array and arg.is_wrapped()
                )

            if valid_arg:
                arg.set_child(self, i)
                self.inputs[i].append(arg)
            else:
                raise TypeError(
                    f"Expected type {t} on argument {i}, but got {type(arg)}"
                )

    def __call__(self, *args: Dependency) -> list[Dependency]:
        self.connect(*args)
        return self.create_output_dependencies()

    def forward(self, other: WorkflowComponent) -> None:
        self.connect(*other.create_output_dependencies())

    def scatter(self, other: WorkflowComponent) -> None:
        values = [
            value.scatter() if isinstance(value, Array) else value
            for value in other.create_output_dependencies()
        ]
        self.connect(*values)

    def gather(self, other: WorkflowComponent) -> None:
        values = [value.wrap() for value in other.create_output_dependencies()]
        self.connect(*values)

    def execute(self, *args: Any, **kwargs: Any) -> Any:
        return self.func(*args, **kwargs)

    # def __str__(self) -> str:
    #     if self.meta:
    #         meta_str = ", ".join(f"{k}={v!r}" for k, v in self.meta.items())
    #     func_source = dedent(inspect.getsource(self.func)).strip()

    #     return (
    #         f"Name: {self.name}\n"
    #         + (f"Metadata: ({meta_str})\n" if self.meta else "")
    #         + f"Function Source:\n{func_source}"
    #     )

    def __repr__(self) -> str:
        return self.name

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
