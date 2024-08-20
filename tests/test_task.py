import pytest
import os

from py2wdl.task import *
from py2wdl.manager import *
from py2wdl.workflow import *


def test_basic_pipeline():
    @task(output_types=(Int, String))
    def first():
        return 1, "test"

    @task(input_types=(Int, String))
    def second(a, b):
        print(a, b)
    
    manager = WorkflowManager()
    manager.add_workflow(first | second)

    assert len(first.outputs[0]) == 1
    assert len(first.outputs[1]) == 1

    first_output_a = first.outputs[0][0]
    first_output_b = first.outputs[1][0]

    assert first_output_a.parent_task == first
    assert first_output_a.output_idx == 0
    assert first_output_a.child_task == second
    assert first_output_a.input_idx == 0

    assert first_output_b.parent_task == first
    assert first_output_b.output_idx == 1
    assert first_output_b.child_task == second
    assert first_output_b.input_idx == 1

    assert len(second.inputs[0]) == 1
    assert len(second.inputs[1]) == 1

    second_input_a = second.inputs[0][0]
    second_input_b = second.inputs[1][0]

    assert first_output_a == second_input_a
    assert first_output_b == second_input_b


def test_value_input():
    @task(input_types=(Int, Int))
    def print_task(a, b):
        print(a + b)

    a, b = Int(1), Int(2)
    
    manager = WorkflowManager()
    manager.add_workflow(Values(a, b) | print_task)

    assert len(print_task.inputs[0]) == 1
    assert len(print_task.inputs[1]) == 1

    print_input_a = print_task.inputs[0][0]
    print_input_b = print_task.inputs[1][0]

    assert print_input_a.parent_task is None
    assert print_input_a.output_idx is None
    assert print_input_a.child_task == print_task
    assert print_input_a.input_idx == 0

    assert print_input_b.parent_task is None
    assert print_input_b.output_idx is None
    assert print_input_b.child_task == print_task
    assert print_input_b.input_idx == 1


def test_fan_out_with_parallel():
    @task(output_types=(Boolean, File))
    def parent_task():
        return True, "test.txt"

    @task(input_types=(Boolean, File))
    def child_task_a(a, b):
        print(a, b)
    
    @task(input_types=(Boolean, File))
    def child_task_b(a, b):
        print(a, b)
    
    manager = WorkflowManager()
    manager.add_workflow(parent_task | ParallelTasks(child_task_a, child_task_b))

    assert len(parent_task.outputs[0]) == 2
    assert len(parent_task.outputs[1]) == 2

    assert parent_task.outputs[0][0].parent_task == parent_task
    assert parent_task.outputs[0][0].output_idx == 0
    assert parent_task.outputs[0][0].child_task == child_task_a
    assert parent_task.outputs[0][0].input_idx == 0

    assert parent_task.outputs[0][1].parent_task == parent_task
    assert parent_task.outputs[0][1].output_idx == 0
    assert parent_task.outputs[0][1].child_task == child_task_b
    assert parent_task.outputs[0][1].input_idx == 0

    assert parent_task.outputs[1][0].parent_task == parent_task
    assert parent_task.outputs[1][0].output_idx == 1
    assert parent_task.outputs[1][0].child_task == child_task_a
    assert parent_task.outputs[1][0].input_idx == 1

    assert parent_task.outputs[1][1].parent_task == parent_task
    assert parent_task.outputs[1][1].output_idx == 1
    assert parent_task.outputs[1][1].child_task == child_task_b
    assert parent_task.outputs[1][1].input_idx == 1

    assert len(child_task_a.inputs[0]) == 1
    assert len(child_task_a.inputs[1]) == 1
    assert len(child_task_b.inputs[0]) == 1
    assert len(child_task_b.inputs[1]) == 1

    assert child_task_a.inputs[0][0] == parent_task.outputs[0][0]
    assert child_task_a.inputs[1][0] == parent_task.outputs[1][0]
    assert child_task_b.inputs[0][0] == parent_task.outputs[0][1]
    assert child_task_b.inputs[1][0] == parent_task.outputs[1][1]


def test_fan_out_with_distributed():
    @task(output_types=(Int, Boolean))
    def parent_task():
        return 1, False
    
    @task(input_types=(Int,))
    def child_task_a(value):
        print(value)
    
    @task(input_types=(Boolean,))
    def child_task_b(value):
        print(value)

    manager = WorkflowManager()
    manager.add_workflow(parent_task | DistributedTasks(child_task_a, child_task_b))    

    assert len(parent_task.outputs[0]) == 1
    assert len(parent_task.outputs[1]) == 1

    assert parent_task.outputs[0][0].parent_task == parent_task
    assert parent_task.outputs[0][0].output_idx == 0
    assert parent_task.outputs[0][0].child_task == child_task_a
    assert parent_task.outputs[0][0].input_idx == 0

    assert parent_task.outputs[1][0].parent_task == parent_task
    assert parent_task.outputs[1][0].output_idx == 1
    assert parent_task.outputs[1][0].child_task == child_task_b
    assert parent_task.outputs[1][0].input_idx == 0

    assert len(child_task_a.inputs[0]) == 1
    assert len(child_task_b.inputs[0]) == 1

    assert child_task_a.inputs[0][0] == parent_task.outputs[0][0]
    assert child_task_b.inputs[0][0] == parent_task.outputs[1][0]

def test_branch_pipeline():
    @task(
        output_types=(Int, Condition, Boolean),
    )
    def branch_task():
        return 1, "child_task_a", True

    @task(input_types=(Int, Boolean))
    def child_task_a(a, b):
        print(a, b)
    
    @task(input_types=(Int, Boolean))
    def child_task_b(a, b):
        print(a, b)

    manager = WorkflowManager()
    manager.add_workflow(branch_task < Tasks(child_task_a, child_task_b))

    assert branch_task.branching
    assert len(branch_task.outputs[0]) == 2
    assert len(branch_task.outputs[2]) == 2

    assert branch_task.outputs[0][0].parent_task == branch_task
    assert branch_task.outputs[0][0].output_idx == 0
    assert branch_task.outputs[0][0].child_task == child_task_a
    assert branch_task.outputs[0][0].input_idx == 0

    assert branch_task.outputs[0][1].parent_task == branch_task
    assert branch_task.outputs[0][1].output_idx == 0
    assert branch_task.outputs[0][1].child_task == child_task_b
    assert branch_task.outputs[0][1].input_idx == 0

    assert branch_task.outputs[2][0].parent_task == branch_task
    assert branch_task.outputs[2][0].output_idx == 2
    assert branch_task.outputs[2][0].child_task == child_task_a
    assert branch_task.outputs[2][0].input_idx == 1

    assert branch_task.outputs[2][1].parent_task == branch_task
    assert branch_task.outputs[2][1].output_idx == 2
    assert branch_task.outputs[2][1].child_task == child_task_b
    assert branch_task.outputs[2][1].input_idx == 1

    assert len(child_task_a.inputs[0]) == 1
    assert len(child_task_a.inputs[1]) == 1
    assert child_task_a.inputs[0][0] == branch_task.outputs[0][0]
    assert child_task_a.inputs[1][0] == branch_task.outputs[2][0]

    assert len(child_task_b.inputs[0]) == 1
    assert len(child_task_b.inputs[1]) == 1
    assert child_task_b.inputs[0][0] == branch_task.outputs[0][1]
    assert child_task_b.inputs[1][0] == branch_task.outputs[2][1]


def test_scatter_pipeline():
    @task(output_types=(Array[Int],))
    def start_task():
        return [1, 2, 3]

    @task(
        input_types=(Int,),
        output_types=(String,),
    )
    def scattered_task_a(value):
        return str(value)

    @task(
        input_types=(String,),
        output_types=(String,),
    )
    def scattered_task_b(value):
        return value

    @task(input_types=(Array[String],))
    def gathered_task(array):
        print(array)
    
    manager = WorkflowManager()
    manager.add_workflow(
        start_task << scattered_task_a | scattered_task_b >> gathered_task
    )

    assert not start_task.is_scattered()
    assert scattered_task_a.is_scattered()
    assert scattered_task_b.is_scattered()
    assert not gathered_task.is_scattered()

    assert len(start_task.outputs[0]) == 1
    assert start_task.outputs[0][0].is_scattered()
    assert start_task.outputs[0][0].parent_task == start_task
    assert start_task.outputs[0][0].output_idx == 0
    assert start_task.outputs[0][0].child_task == scattered_task_a
    assert start_task.outputs[0][0].input_idx == 0
    assert scattered_task_a.inputs[0][0] == start_task.outputs[0][0]
    
    assert len(scattered_task_a.outputs[0]) == 1
    assert not scattered_task_a.outputs[0][0].is_scattered()
    assert scattered_task_a.outputs[0][0].parent_task == scattered_task_a
    assert scattered_task_a.outputs[0][0].output_idx == 0
    assert scattered_task_a.outputs[0][0].child_task == scattered_task_b
    assert scattered_task_a.outputs[0][0].input_idx == 0
    assert scattered_task_b.inputs[0][0] == scattered_task_a.outputs[0][0]

    assert len(scattered_task_b.outputs[0]) == 1
    assert scattered_task_b.outputs[0][0].is_wrapped()
    assert scattered_task_b.outputs[0][0].parent_task == scattered_task_b
    assert scattered_task_b.outputs[0][0].output_idx == 0
    assert scattered_task_b.outputs[0][0].child_task == gathered_task
    assert scattered_task_b.outputs[0][0].input_idx == 0
    assert gathered_task.inputs[0][0] == scattered_task_b.outputs[0][0]


def test_task_to_runnable_script(): 
    @task(input_types=(Int, Boolean))
    def my_task(a, b):
        print(a, b)
    
    manager = WorkflowManager()
    manager.add_workflow(my_task)
    manager.translate()

    with open("my_task.py", "r") as file:
        created = file.read()
    with open("tests/task.to_runnable_script.py", "r") as file:
        desired = file.read()
    
    assert created == desired
    # os.remove("my_task.py")
