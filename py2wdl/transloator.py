import ast
import inspect
from textwrap import dedent

from .task import Task


class Translator:
    def create_runnable_script(self, task: Task) -> None:
        func_source = self.get_func_source(task)
        func_names = self.extract_names(func_source)
        file_source = self.get_file_source(task.func)
        needed_imports = self.extract_needed_imports(file_source, func_names)
        self.write_script(task.name, needed_imports, func_source)

    def get_func_source(self, task: Task) -> str:
        func_source = inspect.getsourcelines(task.func)[0]
        for i, line in enumerate(func_source):
            if line.strip().startswith("def "):
                return dedent("".join(func_source[i:])).strip()
        raise ValueError("Function definition not found in source lines.")

    def extract_names(self, func_source: str) -> set[str]:
        func_tree = ast.parse(func_source)
        return {node.id for node in ast.walk(func_tree) if isinstance(node, ast.Name)}

    def get_file_source(self, func) -> str:
        file_path = inspect.getfile(func)
        with open(file_path, "r") as file:
            return file.read()

    def extract_needed_imports(
        self, file_source: str, func_names: set[str]
    ) -> list[str]:
        file_tree = ast.parse(file_source)
        needed_imports = []

        for node in file_tree.body:
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name in func_names or alias.asname in func_names:
                        import_stmt = f"import {alias.name}"
                        if alias.asname:
                            import_stmt += f" as {alias.asname}"
                        needed_imports.append(import_stmt)
            elif isinstance(node, ast.ImportFrom):
                if node.module in func_names or any(
                    alias.name in func_names for alias in node.names
                ):
                    import_stmt = f"from {node.module} import " + ", ".join(
                        alias.name for alias in node.names
                    )
                    needed_imports.append(import_stmt)

        return needed_imports

    def write_script(
        self, task_name: str, needed_imports: list[str], func_source: str
    ) -> None:
        import_part = "\n".join(needed_imports) + "\n\n\n" if needed_imports else ""
        script_content = import_part + func_source
        with open(f"./{task_name}.py", "w") as file:
            file.write(script_content)
