from __future__ import annotations
import inspect
from textwrap import dedent
from typing import Optional, Callable, Iterable, Union, Type, Any
from typing import TypeVar, Generic
from typing import get_origin, get_args


class WDLValue:
    def __init__(
        self, parent_task: Optional[Task] = None, output_idx: Optional[int] = None
    ) -> None:
        self.name: str = str(id(self))
        self.parent_task: Optional[Task] = parent_task
        self.output_idx: Optional[int] = output_idx
        self.children: list[tuple[Task, int]] = []

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
        self.value: list[Union[bool, int, str]] = value
        self.element = self.element_type(parent_task=parent_task, output_idx=output_idx)

    def __iter__(self) -> Iterable[WDLValue]:
        return iter([self.element])

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
    ) -> None:
        self.func: Callable[..., Any] = func
        self.name: str = name
        self.input_types = input_types
        self.meta: Optional[dict[str, Any]] = meta
        self.outputs: list[WDLValue] = []
        if output_types is not None:
            self.setting_output_values(output_types)

    def setting_output_values(self, output_types: Iterable[Type[WDLValue]]):
        for i, output_type in enumerate(output_types):
            if get_origin(output_type) is Array:
                element_type = get_args(output_type)[0]
                output = output_type(
                    parent_task=self, output_idx=i, element_type=element_type
                )
            else:
                output = output_type(parent_task=self, output_idx=i)
            self.outputs.append(output)

    def __call__(self, *args: WDLValue) -> Any:
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

        output_length = len(self.outputs)
        if output_length == 0:
            return None
        elif output_length == 1:
            return self.outputs[0]
        else:
            return self.outputs

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

    def __or__(self, other_task: Task) -> Task:
        if not isinstance(other_task, Task):
            raise TypeError(f"Expected Task but got {type(other_task)}")
        
        other_task(*self.outputs)
        return self
    
    def __ror__(self, values: list[WDLValue]) -> Task:
        if not all(isinstance(value, WDLValue) for value in values):
            raise TypeError(f"Expected list of WDLValue but got {type(values)}")
        
        self(*values)
        return self


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
