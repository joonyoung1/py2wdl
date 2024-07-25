from __future__ import annotations
import inspect
from textwrap import dedent
from typing import Optional, Callable, Iterable, Type, Any, TypeVar, Generic


class WDLValue:
    def __init__(
        self, parent_task: Optional[Task] = None, output_idx: Optional[int] = None
    ) -> None:
        self.name: str = str(id(self))
        self.parent_task: Optional[Task] = parent_task
        self.output_idx: Optional[int] = output_idx
        self.child: list[tuple[Task, int]] = []

    def add_child(self, child_task: Task, input_idx: int) -> None:
        self.child.append((child_task, input_idx))


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
        value: Optional[bool] = None,
        parent_task: Optional[Task] = None,
        output_idx: Optional[int] = None,
    ) -> None:
        super().__init__(parent_task, output_idx)
        self.value: Optional[int] = value


class String(WDLValue):
    def __init__(
        self,
        value: Optional[bool] = None,
        parent_task: Optional[Task] = None,
        output_idx: Optional[int] = None,
    ) -> None:
        super().__init__(parent_task, output_idx)
        self.value: Optional[str] = value


class File(String): ...


class Condition(String): ...


T = TypeVar('T', Boolean, Int, String, File)


class Array(WDLValue, Generic[T]):
    def __init__(
        self,
        value: list[WDLValue] = [],
        parent_task: Optional[Task] = None,
        output_idx: Optional[int] = None,
    ) -> None:
        super().__init__(parent_task, output_idx)
        self.value: Optional[list[WDLValue]] = value
        self._type: Type[WDLValue] = T
    
    def __iter__(self) -> Iterable[WDLValue]:
        return iter(self.value)

    def get_element_type(self) -> Type[WDLValue]:
        return self._type


class Task:
    def __init__(
        self,
        func: Callable[..., Any],
        name: str,
        input_types: Optional[Iterable[Type[WDLValue]]] = None,
        output_types: Optional[Iterable[Type[WDLValue]]] = None,
        meta: Optional[dict[str, Any]] = None,
    ) -> None:
        self.func: Callable[..., Any] = func
        self.name: str = name
        self.input_types = input_types
        self.output_values = tuple(
            t(parent_task=self, output_idx=i) for i, t in enumerate(output_types)
        )
        self.meta: Optional[dict[str, Any]] = meta

    def __call__(self, *args: WDLValue) -> Any:
        for i, (arg, t) in enumerate(zip(args, self.input_types)):
            if not isinstance(arg, t):
                raise TypeError(
                    f"Expected type {t} on parameter {i}, but got {type(arg)}"
                )
            else:
                arg.set_input(self, i)

        if len(self.output_values) > 1:
            return self.output_values
        else:
            return self.output_values[0]

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
    input_types: Optional[Iterable[Type[WDLValue]]] = None,
    output_types: Optional[Iterable[Type[WDLValue]]] = None,
    meta: Optional[dict[str, Any]] = None,
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
        name="adding_task",
        input_types=(File, Int),
        output_types=(Int,),
        meta={"description": "simple adding task"},
    )
    def adder(a, b):
        return a + b

    print(adder)

    adder_input_a = File()
    adder_input_b = Int()
    adder_output = adder(adder_input_a, adder_input_b)

    print(adder_output)
