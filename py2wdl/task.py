from __future__ import annotations
import inspect
from textwrap import dedent
from typing import Callable, Any, Dict


class Task:
    def __init__(
        self,
        func: Callable[..., Any],
        input_type: str | None = None,
        output_type: str | None = None,
        meta: Dict[str, Any] | None = None,
    ) -> None:
        self.func = func
        self.input_type = input_type
        self.output_type = output_type
        self.meta = meta

    def execute(self, *args: Any, **kwargs: Any) -> Any:
        return self.func(*args, **kwargs)

    def __str__(self) -> str:
        if self.meta:
            meta_str = ", ".join(f"{k}={v!r}" for k, v in self.meta.items())
        func_source = dedent(inspect.getsource(self.func)).strip()

        return (
            f"Metadata: ({meta_str})\n" if self.meta else ""
        ) + f"Function Source:\n{func_source}"


def task(
    input_type: str | None = None,
    output_type: str | None = None,
    meta: Dict[str, Any] | None = None,
):
    def task_factory(func):
        return Task(
            func=func,
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
