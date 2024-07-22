import inspect
from textwrap import dedent
from typing import Callable, Any, Dict, Tuple


class Task:
    def __init__(
        self, func: Callable[..., Any], meta: Dict[str, Any], *args: Any, **kwargs: Any
    ) -> None:
        self.func = func
        self.meta = meta
        self.args = args
        self.kwargs = kwargs

    def execute(self) -> Any:
        return self.func(*self.args, **self.kwargs)

    def __str__(self) -> str:
        args_str = ", ".join(repr(arg) for arg in self.args)
        kwargs_str = ", ".join(f"{k}={v!r}" for k, v in self.kwargs.items())
        func_source = dedent(inspect.getsource(self.func)).strip()

        return (
            f"Arguments: ({args_str})\n"
            f"Keyword Arguments: ({kwargs_str})\n"
            f"Function Source:\n{func_source}"
        )


class TaskFactory:
    def __init__(self, func: Callable[..., Any], meta: Dict[str, Any]) -> None:
        self.func = func
        self.meta = meta

    def __call__(self, *args: Any, **kwargs: Any) -> Task:
        return Task(self.func, self.meta, *args, **kwargs)


class TaskDecorator:
    def __init__(self, **kwargs: Any) -> None:
        self.meta = kwargs

    def __call__(self, func: Callable[..., Any]) -> TaskFactory:
        return TaskFactory(func, self.meta)


if __name__ == "__main__":

    @TaskDecorator()
    def temp(*args: Any, **kwargs: Any) -> None:
        print(f"args = {args}, kwargs = {kwargs}")

    task = temp(1, value=3)
    task.execute()
    print(task)
