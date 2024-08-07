from __future__ import annotations
import inspect
from textwrap import dedent
from typing import Optional, Callable, Iterable, Union, Type, Any
from typing import TypeVar, Generic
from typing import get_origin, get_args


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
        self.scattered: bool = False

    def add_child(self, child_task: Task, input_idx: int) -> None:
        self.children.append((child_task, input_idx))

    def use_scatter(self) -> None:
        self.scattered = True

    def is_scattered(self) -> bool:
        return self.scattered

    def wrap(self) -> Array:
        return Array(
            element_type=type(self),
            parent_task=self.parent_task,
            output_idx=self.output_idx,
        )


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
        self.value: list[Union[bool, int, str]] = value
        self.element: WDLValue = self.element_type(
            parent_task=parent_task, output_idx=output_idx
        )

    def get_element_type(self) -> Type[WDLValue]:
        return self.element_type


class Task:
    def __init__(
        self,
        func: Callable[..., Any],
        name: str,
        input_types: Iterable[Type[WDLValue]] = (),
        output_types: Iterable[Type[WDLValue]] = (),
        meta: dict[str, Any] = {},
        branch: bool = False,
    ) -> None:
        self.func: Callable[..., Any] = func
        self.name: str = name
        self.meta: Optional[dict[str, Any]] = meta

        self.input_types: Iterable[Type[WDLValue]] = input_types
        self.outputs: list[WDLValue] = []
        if output_types is not None:
            self.setting_output_values(output_types)

        self.branch: bool = branch
        self.condition: Condition
        if branch:
            if Condition not in output_types:
                raise TypeError("BranchTask must include a Condition in its output")

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
        if self.branch:
            return [output for output in self.outputs if type(output) != Condition]
        else:
            return self.outputs

    def __call__(self, *args: WDLValue) -> Union[WDLValue, list[WDLValue]]:
        if len(args) != len(self.input_types):
            raise TypeError(
                f"Expected {len(self.input_types)} arguments but got {len(args)}"
            )

        for i, (arg, t) in enumerate(zip(args, self.input_types)):
            if not isinstance(arg, t):
                raise TypeError(
                    f"Expected type {t} on argument {i}, but got {type(arg)}."
                )
            else:
                arg.add_child(self, i)

        return self.get_outputs()

    def __or__(self, other: Union[Task, list[Task], tuple[Task]]) -> Task:
        if isinstance(other, Task):
            other(*self.outputs)
            return other

        elif all(isinstance(task, Task) for task in other):
            if isinstance(other, list):
                for task in other:
                    task(*self.outputs)

            elif isinstance(other, tuple):
                i = 0
                for task in other:
                    length = len(task.input_types)
                    task(*self.outputs[i : i + length])
                    i += length

            return other
        else:
            raise TypeError(f"Expected Task but got {type(other)}")

    def __ror__(self, other: Union[list[WDLValue], list[Task]]) -> Task:
        if all(isinstance(value, WDLValue) for value in other):
            self(*other)
            return self

        elif all(isinstance(task, Task) for task in other):
            values = []
            for task in other:
                values.extend(task.get_outputs())
            self(*values)
            return self

        else:
            raise TypeError(f"Expected list of Task or WDLValue but got {type(other)}")
    
    def __lshift__(self, other: Task) -> Task:
        if not all(isinstance(value, Array) for value in self.outputs):
            raise ValueError("All outputs must be of type Array for scatter operation")

        outputs = []
        for output in self.outputs:
            element = output.element
            element.use_scatter()
            output.append(element)

        other(*outputs)
        return other

    def __rshift__(self, other: Task) -> Task:
        if not all(element.is_scattered() for element in self.outputs):
            raise ValueError("All outputs must be scattered for gathar operation")
        
        other(*[output.wrap() for output in self.outputs])
        return other

    def __gt__(self, other: list[Task]) -> list[Task]:
        outputs = self.get_outputs()
        for task in other:
            task(*outputs)

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
    branch: bool = False,
) -> Callable[..., Any]:
    def task_factory(func: Callable[..., Any]) -> Task:
        return Task(
            func=func,
            name=name if name else func.__name__,
            input_types=input_types,
            output_types=output_types,
            meta=meta,
            branch=branch
        )

    return task_factory


if __name__ == "__main__":

    @task(
        input_types=(Int,),
        output_types=(Array[Int],),
    )
    def task_a(num: int):
        my_array = [i for i in range(num)]
        return my_array

    @task(
        input_types=(Int,),
    )
    def task_b(num: int):
        print(num)

    task_a_input = Int(value=10)
    generated_array = task_a(task_a_input)
    for num in generated_array:
        task_b(num)
