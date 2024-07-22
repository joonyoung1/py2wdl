import inspect
from textwrap import dedent


class Task:
    def __init__(self, func, meta, *args, **kwargs):
        self.func = func
        self.meta = meta
        self.args = args
        self.kwargs = kwargs

    def execute(self):
        return self.func(*self.args, **self.kwargs)

    def __str__(self):
        args_str = ', '.join(repr(arg) for arg in self.args)
        kwargs_str = ', '.join(f"{k}={v!r}" for k, v in self.kwargs.items())
        func_source = dedent(inspect.getsource(self.func)).strip()

        return (
            f"Arguments: ({args_str})\n"
            f"Keyword Arguments: ({kwargs_str})\n"
            f"Function Source:\n{func_source}"
        )


class TaskFactory:
    def __init__(self, func, meta):
        self.func = func
        self.meta = meta

    def __call__(self, *args, **kwargs):
        return Task(self.func, self.meta, *args, **kwargs)


class TaskDecorator:
    def __init__(self, **kwargs):
        self.meta = kwargs

    def __call__(self, func):
        return TaskFactory(func, self.meta)


if __name__ == "__main__":

    @TaskDecorator()
    def temp(*args, **kwargs):
        print(f"args = {args}, kwargs = {kwargs}")

    task = temp(1, value=3)
    task.execute()
    print(task)
