import ast
import inspect
from textwrap import dedent

from .task import Task, Int, Float, Boolean


class Translator:
    def create_runnable_script(self, task: Task) -> None:
        func_source = inspect.getsourcelines(task.func)[0]
        for i, line in enumerate(func_source):
            if line.strip().startswith("def "):
                func_source = dedent("".join(func_source[i:])).strip()
                break
        else:
            raise ValueError("Function definition not found in source lines.")

        func_tree = ast.parse(func_source)
        func_names = {
            node.id for node in ast.walk(func_tree) if isinstance(node, ast.Name)
        }

        file_path = inspect.getfile(task.func)
        with open(file_path, "r") as file:
            file_source = file.read()
        file_tree = ast.parse(file_source)

        needed_imports = self.parse_needed_imports(file_tree, func_names)
        if "import sys" not in needed_imports:
            needed_imports.append("import sys")
        import_block = "\n".join(needed_imports) + "\n\n\n" if needed_imports else ""

        main_block = '\n\n\nif __name__ == "__main__":\n'
        for i, input_type in enumerate(task.input_types, 1):
            if input_type == Int:
                main_block += f"\tsys.args[{i}] = int(sys.args[{i}])\n"
            elif input_type == Float:
                main_block += f"\tsys.args[{i}] = float(sys.args[{i}])\n"
            elif input_type == Boolean:
                main_block += (
                    f'\tsys.args[{i}] = True if sys.args[{i}] == "true" else False\n'
                )
        main_block += f"\t{task.name}(*sys.args[1:])"

        script_content = import_block + func_source + main_block
        with open(f"./{task.name}.py", "w") as file:
            file.write(script_content)

    def parse_needed_imports(
        self, file_tree: ast.Module, func_names: set[str]
    ) -> list[str]:
        
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
