import ast
import inspect
from textwrap import dedent

from .task import Task, Int, Float, Boolean
from .task import Values, Tasks
from .workflow import WorkflowComponent


class Translator:
    def __init__(self, indentation: str = "    ") -> None:
        self.ind: str = indentation

    def generate_runnable_script(self, task: Task) -> None:
        func_source = self.parse_func_source(task)
        file_source = self.parse_file_source(task)
        import_block = self.generate_import_block(func_source, file_source)
        main_block = self.generate_main_block(task)

        script_content = import_block + func_source + main_block
        with open(f"./{task.name}.py", "w") as file:
            file.write(script_content)

    def parse_func_source(self, task: Task) -> str:
        func_source = inspect.getsourcelines(task.func)[0]
        for i, line in enumerate(func_source):
            if line.strip().startswith("def "):
                func_source = dedent("".join(func_source[i:])).strip()
                break
        else:
            raise ValueError("Function definition not found in source lines.")
        return func_source

    def parse_file_source(self, task: Task) -> str:
        file_path = inspect.getfile(task.func)
        with open(file_path, "r") as file:
            file_source = file.read()
        return file_source

    def generate_import_block(self, func_source: str, file_source: str) -> list[str]:
        func_tree = ast.parse(func_source)
        func_names = {
            node.id for node in ast.walk(func_tree) if isinstance(node, ast.Name)
        }

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

        if "import sys" not in needed_imports:
            needed_imports.append("import sys")
        return "\n".join(needed_imports) + "\n\n\n" if needed_imports else ""

    def generate_main_block(self, task: Task) -> str:
        main_block = '\n\n\nif __name__ == "__main__":\n'

        for i, input_type in enumerate(task.input_types, 1):
            if input_type == Int:
                main_block += self.ind + f"sys.args[{i}] = int(sys.args[{i}])\n"
            elif input_type == Float:
                main_block += self.ind + f"sys.args[{i}] = float(sys.args[{i}])\n"
            elif input_type == Boolean:
                main_block += (
                    self.ind
                    + f'sys.args[{i}] = True if sys.args[{i}] == "true" else False\n'
                )

        main_block += (
            f"{self.ind}outputs = {task.name}(*sys.args[1:])\n\n"
            f"{self.ind}for i, output in enumerate(outputs):\n"
            f'{self.ind*2}with open(f"{task.name}_output_{{i}}.txt", "w") as file:\n'
            f"{self.ind*3}if isinstance(output, list):\n"
            f'{self.ind*4}file.write("\\n".join(map(str, output)))\n'
            f"{self.ind*3}else:\n"
            f"{self.ind*4}file.write(str(output))\n"
        )
        return main_block

    def generate_task_definition_wdl(self, task: Task) -> None:
        input_block = self.generate_input_block(task)
        command_block = self.generate_command_block(task)
        output_block = self.generate_output_block(task)

        script = (
            f"task {task.name} {{\n"
            f"{self.ind}input {{\n"
            f"{input_block}\n"
            f"{self.ind}}}\n\n"
            f"{self.ind}command {{\n"
            f"{command_block}\n"
            f"{self.ind}}}\n"
        )

        if len(task.output_types) > 0:
            script += (
                f"\n{self.ind}output {{\n"
                f"{output_block}\n"
                f"{self.ind}}}\n"
            )
        script += "}\n\n"

        with open("wdl_script.wdl", "a") as file:
            file.write(script)

    def generate_input_block(self, task: Task) -> str:
        input_lines = [
            f"{self.ind * 2}{input_type.repr()} input_{i}"
            for i, input_type in enumerate(task.input_types)
        ]
        return "\n".join(input_lines)

    def generate_command_block(self, task: Task) -> str:
        command_args = " ".join(
            f"${{{task.name}_input_{i}}}" for i in range(len(task.input_types))
        )
        return f"{self.ind * 2}python {task.name}.py {command_args}"

    def generate_output_block(self, task: Task) -> str:
        output_lines = []
        for i, output_type in enumerate(task.outputs):
            var_name = f"{task.name}_output_{i}"
            type_repr = output_type.repr()
            line = f"{self.ind*2}{type_repr} {var_name} = read_{type_repr.lower()}({var_name}.txt)"
            output_lines.append(line)

        return "\n".join(output_lines)

    def generate_workflow_definition_wdl(
        self, components: set[WorkflowComponent]
    ) -> None:
        values_list = []
        tasks = []
        for component in components:
            if isinstance(component, Values):
                values_list.append(component)
            elif isinstance(component, Tasks):
                tasks.extend(component.tasks)
            else:
                tasks.append(component)

        script = "workflow my_workflow {\n"
        for task in tasks:
            input_block = f"{self.ind}call {task.name} {{\n{self.ind*2}input:\n"

            for i, inp in enumerate(task.inputs):
                input_line = f"{self.ind*3}input_{i} = "
                if isinstance(inp[0].parent, Task):
                    input_line += f"{inp[0].parent.name}.output_{inp[0].output_idx},\n"
                elif isinstance(inp[0].parent, Values):
                    input_line += f"{inp[0].parent.name}_output_{inp[0].output_idx},\n"

                input_block += input_line
            script += input_block + f"{self.ind}}}\n"
        script += "}\n"

        with open("wdl_script.wdl", "a") as file:
            file.write(script)

    def generate_workflow_input_wdl(self, components: list[Values]) -> None:
        for values in components:
            ...
