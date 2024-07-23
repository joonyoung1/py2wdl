from __future__ import annotations
import inspect
from textwrap import dedent
from typing import Optional, Callable, Any


class Task:
    def __init__(
        self,
        func: Callable[..., Any],
        name: str,
        input_type: Optional[str] = None,
        output_type: Optional[str] = None,
        meta: Optional[dict[str, Any]] = None,
    ) -> None:
        self.func: Callable[..., Any] = func
        self.name: str = name
        self.input_type = input_type
        self.output_type = output_type
        self.meta: Optional[dict[str, Any]] = meta

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
    input_type: Optional[str] = None,
    output_type: Optional[str] = None,
    meta: Optional[dict[str, Any]] = None,
) -> Callable[..., Any]:
    def task_factory(func: Callable[..., Any]) -> Task:
        return Task(
            func=func,
            name=name if name else func.__name__,
            input_type=input_type,
            output_type=output_type,
            meta=meta,
        )

    return task_factory


if __name__ == "__main__":

    @task(meta={"name": "adding_task"})
    def adder(a: int, b: int) -> None:
        return a + b

    print(adder)
    print(adder.execute(3, 5))
