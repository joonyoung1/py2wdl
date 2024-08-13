import ast
import inspect
from textwrap import dedent

from .workflow import Workflow, WorkflowComponent
from .task import Task, Tasks


class WorkflowManager:
    def __init__(self) -> None:
        self.components: set[WorkflowComponent] = set()

    def add_workflow(self, workflow: Workflow) -> None:
        self.components.update(workflow.operands)

        base = workflow.operands[0]
        for other, operator in zip(workflow.operands[1:], workflow.operators):
            outputs = base.get_outputs()

            if operator == "|":
                if base.is_scattered():
                    other.use_scatter()
                other.forward(outputs)

            elif operator == "<":
                if base.is_scattered():
                    other.use_scatter()
                if not isinstance(base, Task) or base.condition is None:
                    raise ValueError(
                        "Task need to return Condition for branch operation"
                    )
                other.branch(outputs)

            elif operator == "<<":
                other.use_scatter()
                other.scatter(outputs)

            elif operator == ">>":
                if not base.is_scattered():
                    raise ValueError(
                        f"WorkflowComponent need to be scattered for gather operation"
                    )
                other.gather(outputs)
            else:
                raise ValueError(f"Unsupported operator")

            base = other

    def translate(self) -> None:
        root_components = []
        for task in self.iterate_over_task():
            self.create_runnable_script(task)

    def iterate_over_task(self):
        for component in self.components:
            if isinstance(component, Task):
                yield component
            elif isinstance(component, Tasks):
                yield from component

    def create_runnable_script(self, task: Task) -> None:
        func_source = self.get_func_source(task)
        func_tree = ast.parse(func_source)
        func_names = {
            node.id for node in ast.walk(func_tree) if isinstance(node, ast.Name)
        }

        with open(inspect.getfile(task.func), "r") as file:
            file_source = file.read()
        file_tree = ast.parse(file_source)

        needed_imports = []
        for node in file_tree.body:
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name in func_names or alias.asname in func_names:
                        needed_imports.append(
                            f"import {alias.name}"
                            + (f" as {alias.asname}" if alias.asname else "")
                        )
            elif isinstance(node, ast.ImportFrom):
                if node.module in func_names or any(
                    alias.name in func_names for alias in node.names
                ):
                    needed_imports.append(
                        f"from {node.module} import "
                        + ", ".join(alias.name for alias in node.names)
                    )

        import_part = "\n".join(needed_imports) + "\n\n\n" if needed_imports else ""
        task_source = import_part + func_source
        with open(f"./{task.name}.py", "w") as file:
            file.write(task_source)

    def get_func_source(self, task: Task) -> str:
        func_source = inspect.getsourcelines(task.func)[0]

        for i, line in enumerate(func_source):
            if line.strip().startswith("def "):
                return dedent("".join(func_source[i:])).strip()

        raise ValueError("Function definition not found in source lines.")
