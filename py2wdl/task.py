from __future__ import annotations
import inspect
from textwrap import dedent
from typing import Optional, Callable, Iterable, Type, Any
from wdl_types import WDLType, Boolean, Int, String, File


class Task:
    def __init__(
        self,
        func: Callable[..., Any],
        name: str,
        input_types: Optional[Iterable[Type[WDLType]]] = None,
        output_types: Optional[Iterable[Type[WDLType]]] = None,
        meta: Optional[dict[str, Any]] = None,
    ) -> None:
        self.func: Callable[..., Any] = func
        self.name: str = name
        self.input_types = input_types
        self.output_types = output_types
        self.meta: Optional[dict[str, Any]] = meta

    def __call__(self, *args: Any) -> Any:
        for i, (received, expected) in enumerate(zip(args, self.input_types)):
            if not isinstance(received, expected):
                raise TypeError(
                    f"Expected type {expected} on parameter {i}, but got {type(received)}"
                )

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
    input_types: Optional[Iterable[Type[WDLType]]] = None,
    output_types: Optional[Iterable[Type[WDLType]]] = None,
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
        input_types=(Int, Int),
        output_types=(Int),
        meta={"description": "simple adding task"},
    )
    def adder(a: int, b: int) -> int:
        return a + b

    print(adder)

    adder_input_a = File()
    adder_input_b = Int()
    adder(adder_input_a, adder_input_b)
