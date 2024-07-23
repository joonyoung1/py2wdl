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
        self.args = []
        self.kwargs = {}

    def set_args(self, *args: Any, **kwargs: Any) -> None:
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


def task(
    input_type: str | None = None,
    output_type: str | None = None,
    meta: Dict[str, Any] | None = None,
):
    def task_factory(func):
        def create_task(*args, **kwargs):
            task_instance = Task(
                func=func,
                input_type=input_type,
                output_type=output_type,
                meta=meta,
            )
            task_instance.set_args(*args, **kwargs)
            return task_instance

        return create_task

    return task_factory


if __name__ == "__main__":

    @task()
    def adder(a: int, b: int) -> None:
        return a + b

    my_task = adder(3, 5)
    print(my_task.execute())
    print(my_task)
