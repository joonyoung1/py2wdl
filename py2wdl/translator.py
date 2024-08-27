import ast
import inspect
from textwrap import dedent
from typing import Union

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
            if single_type_repr == "condition":
                single_type_repr = "string"
                type_repr = "String"
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
        values_list = set()
        tasks = set()
        for component in components:
            if isinstance(component, Values):
                values_list.add(component)
            elif isinstance(component, Tasks):
                tasks.update(component.tasks)
            else:
                tasks.add(component)
        self.set_call_scripts(tasks)

        self.generate_workflow_input_wdl(values_list)

        contents = self.sort_tasks(tasks)
        script = ""
        for content in contents:
            if isinstance(content, Task):
                script += (
                    "\n".join(
                        [
                            self.ind * content.lv + line
                            for line in content.call_script.splitlines()
                        ]
                    )
                    + "\n"
                )
            else:
                script += content

        script = (
            "workflow my_workflow {\n"
            + self.generate_workflow_input_wdl(values_list)
            + script
            + "}\n"
        )

        with open("wdl_script.wdl", "a") as file:
            file.write(script)

    def init_wdl_script(self) -> None:
        with open("wdl_script.wdl", "w") as file:
            pass

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

            else:
                for i, inps in enumerate(task.inputs):
                    call_script += f"{self.ind*2}input_{i} = {task.name}_input_{i},\n"

                    for inp in inps:
                        print(inp.parent)
                        if not isinstance(inp.parent, Task):
                            raise TypeError(
                                "Inputs from multiple sources must be received through a branched Task."
                            )
                        inp.parent.call_script += f"{task.name}_input_{i} = {inp.parent.name}.output_{inp.output_idx}\n"
            task.call_script = call_script + "}\n" + task.call_script

    def sort_tasks(self, tasks: list[Task]) -> list[Union[Task, str]]:
        defined_tasks = set()
        contents = []

        while len(defined_tasks) < len(tasks):
            for task in tasks:
                deps = [dep for input in task.inputs for dep in input]
                parents = list(set([dep.parent for dep in deps]))
                if not all(
                    isinstance(parent, Values) or parent in defined_tasks
                    for parent in parents
                ):
                    continue
                elif task in defined_tasks:
                    continue

                defined_tasks.add(task)

                if len(parents) == 0:
                    task.lv = 1
                    contents.insert(0, task)

                elif all(len(input) > 1 for input in task.inputs):
                    if not all(parent.lv == parents[0].lv for parent in parents):
                        raise ValueError("Input sources has different level")
                    task.lv = parents[0].lv - 1
                    max_idx = max(contents.index(parent) for parent in parents)
                    contents.insert(max_idx + 2, task)

                elif task.is_scattered():
                    for dep in deps:
                        if not dep.parent.is_scattered() and dep.is_scattered():
                            task.lv = dep.parent.lv + 1
                            idx = contents.index(dep.parent)
                            contents.insert(idx + 1, "scatter")
                            contents.insert(idx + 2, task)
                            break
                    else:
                        max_lv_parent = max(parents, key=lambda x: x.lv)
                        idx = contents.index(max_lv_parent)
                        contents.insert(idx, task)

                elif not task.is_scattered():
                    for dep in deps:
                        if dep.parent.is_scattered() and dep.is_wrapped():
                            task.lv = dep.parent.lv - 1
                            idx = contents.index(dep.parent)
                            contents.insert(idx + 1, task)
                            break
                    else:
                        task.lv = parents[0].lv
                        max_idx = max(
                            (
                                contents.index(parent)
                                if not isinstance(parent, Values)
                                else 0
                            )
                            for parent in parents
                        )
                        contents.insert(max_idx, task)

                if task.branching:
                    idx = contents.index(task)
                    children = set(
                        dep.child for output in task.outputs for dep in output
                    )

                    first_line = True
                    while children:
                        child = children.pop()
                        defined_tasks.add(child)
                        child.lv = task.lv + 1

                        if_state = "if" if first_line else "else if"
                        first_line = False

                        contents.insert(
                            idx + 1,
                            f'{self.ind*task.lv}{if_state} {task.name}.output_{task.cond_idx} == "{child.name}" {{\n',
                        )
                        contents.insert(idx + 2, child)
                        contents.insert(idx + 3, f"{self.ind*task.lv}}}\n")
                        idx += 3

        return contents

    def generate_workflow_input_wdl(self, components: list[Values]) -> None:
        input_block = f"{self.ind}input {{\n"
        for values in components:
            for i, value in enumerate(values):
                input_block += f"{self.ind*2}{format_type_hint(type(value))} {values.name}_output_{i} = {value.repr()}\n"
        input_block += f"{self.ind}}}\n"
        return input_block
