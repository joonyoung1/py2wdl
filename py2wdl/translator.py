import ast
import inspect
from textwrap import dedent

from .task import Task, Int, Float, Boolean, format_type_hint
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
            f"{input_block}"
            f"{command_block}"
            f"{output_block}"
            f"}}\n"
        )

        with open("wdl_script.wdl", "a") as file:
            file.write(script)

    def generate_input_block(self, task: Task) -> str:
        input_lines = [
            f"{self.ind * 2}{format_type_hint(input_type)} input_{i}"
            for i, input_type in enumerate(task.input_types)
        ]

        if input_lines:
            joined_lines = "\n".join(input_lines)
            input_block = f"{self.ind}input {{\n{joined_lines}\n{self.ind}}}\n"
        else:
            input_block = ""
        return input_block

    def generate_command_block(self, task: Task) -> str:
        command_args = " ".join(f"${{input_{i}}}" for i in range(len(task.input_types)))
        command_line = f"{self.ind * 2}python {task.name}.py {command_args}"
        return f"{self.ind}command {{\n{command_line}\n{self.ind}}}\n"

    def generate_output_block(self, task: Task) -> str:
        output_lines = []
        for i, output_type in enumerate(task.output_types):
            var_name = f"{task.name}_output_{i}"
            type_repr = format_type_hint(output_type)
            single_type_repr = output_type.__name__.lower()
            line = f"{self.ind*2}{type_repr} {var_name} = read_{single_type_repr}({var_name}.txt)"
            output_lines.append(line)

        if output_lines:
            joined_lines = "\n".join(output_lines)
            output_block = f"{self.ind}output {{\n{joined_lines}\n{self.ind}}}\n"
        else:
            output_block = ""
        return output_block

    def generate_workflow_definition_wdl(
        self, components: set[WorkflowComponent]
    ) -> None:
        self.init_wdl_script()

        values_list = set()
        tasks = set()
        for component in components:
            if isinstance(component, Values):
                values_list.add(component)
            elif isinstance(component, Tasks):
                tasks.update(component.tasks)
            else:
                tasks.add(component)

        self.set_priorities(tasks)
        tasks = list(tasks)
        tasks.sort(key=lambda task: task.priority)
        self.set_call_scripts(tasks)

        script = ""
        for task in tasks:
            script += task.call_script

        with open("wdl_script.wdl", "a") as file:
            file.write(script)
    
    def init_wdl_script(self) -> None:
        with open("wdl_script.wdl", "w") as file:
            pass

    def generate_workflow_input_wdl(self, components: list[Values]) -> None:
        for values in components:
            ...

    def set_priorities(self, tasks: list[Task]) -> None:
        while True:
            updated = False
            for task in tasks:
                if task.priority != -1:
                    continue

                if len(task.inputs) == 0:
                    task.priority = 0
                    continue

                task.priority = self.calc_priority(task)
                updated = True
            
            if not updated:
                break
    
    def calc_priority(self, task: Task) -> int:
        max_priority = -1
        for input in task.inputs:
            for dep in input:
                if dep.parent.priority == -1:
                    return -1
                
                max_priority = max(max_priority, dep.parent.priority)
        return max_priority + 1

    def set_call_scripts(self, tasks: list[Task]) -> None:
        for task in tasks:
            if len(task.input_types) == 0:
                task.call_script = f"call {task.name}\n"
                continue

            call_script = f"call {task.name} {{\n{self.ind}input:\n"

            if all(len(inp) == 1 for inp in task.inputs):
                for i, inp in enumerate(task.inputs):
                    input_line = f"{self.ind*2}input_{i} = "
                    if isinstance(inp[0].parent, Task):
                        input_line += (
                            f"{inp[0].parent.name}.output_{inp[0].output_idx},\n"
                        )
                    elif isinstance(inp[0].parent, Values):
                        input_line += (
                            f"{inp[0].parent.name}_output_{inp[0].output_idx},\n"
                        )

                    call_script += input_line
                task.call_script = call_script + "}\n"

            else:
                for i, inps in enumerate(task.inputs):
                    call_script += f"{self.ind*2}input_{i} = {task.name}_input_{i},\n"

                    for inp in inps:
                        if not isinstance(inp.parent, Task):
                            raise TypeError(
                                "Inputs from multiple sources must be received through a branched Task."
                            )
                        inp.parent.call_script += f"{task.name}_input_{i} = {inp.parent.name}.output_{inp.output_idx}\n"
