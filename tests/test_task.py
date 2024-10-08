import pytest
import os

from py2wdl.task import *
from py2wdl.manager import *
from py2wdl.workflow import *
from py2wdl.operator import *


def test_basic_pipeline():
    @task(output_types=(Int, String))
    def first():
        return 1, "test"

    @task(input_types=(Int, String))
    def second(a, b):
        print(a, b)
    
    manager = WorkflowManager()
    manager.add_workflow(first |f| second)

    assert len(first.outputs[0]) == 1
    assert len(first.outputs[1]) == 1

    first_output_a = first.outputs[0][0]
    first_output_b = first.outputs[1][0]

    assert first_output_a.parent == first
    assert first_output_a.output_idx == 0
    assert first_output_a.child == second
    assert first_output_a.input_idx == 0

    assert first_output_b.parent == first
    assert first_output_b.output_idx == 1
    assert first_output_b.child == second
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
    values = Values(a, b)
    manager.add_workflow(values |f| print_task)

    assert len(print_task.inputs[0]) == 1
    assert len(print_task.inputs[1]) == 1

    assert print_task.inputs[0][0].parent is values
    assert print_task.inputs[0][0].output_idx == 0
    assert print_task.inputs[0][0].child == print_task
    assert print_task.inputs[0][0].input_idx == 0

    assert print_task.inputs[1][0].parent is values
    assert print_task.inputs[1][0].output_idx == 1
    assert print_task.inputs[1][0].child == print_task
    assert print_task.inputs[1][0].input_idx == 1


def test_fan_out_with_parallel():
    @task(output_types=(Boolean, File))
    def parent():
        return True, "test.txt"

    @task(input_types=(Boolean, File))
    def child_a(a, b):
        print(a, b)
    
    @task(input_types=(Boolean, File))
    def child_b(a, b):
        print(a, b)
    
    manager = WorkflowManager()
    manager.add_workflow(parent |f| ParallelTasks(child_a, child_b))

    assert len(parent.outputs[0]) == 2
    assert len(parent.outputs[1]) == 2

    assert parent.outputs[0][0].parent == parent
    assert parent.outputs[0][0].output_idx == 0
    assert parent.outputs[0][0].child == child_a
    assert parent.outputs[0][0].input_idx == 0

    assert parent.outputs[0][1].parent == parent
    assert parent.outputs[0][1].output_idx == 0
    assert parent.outputs[0][1].child == child_b
    assert parent.outputs[0][1].input_idx == 0

    assert parent.outputs[1][0].parent == parent
    assert parent.outputs[1][0].output_idx == 1
    assert parent.outputs[1][0].child == child_a
    assert parent.outputs[1][0].input_idx == 1

    assert parent.outputs[1][1].parent == parent
    assert parent.outputs[1][1].output_idx == 1
    assert parent.outputs[1][1].child == child_b
    assert parent.outputs[1][1].input_idx == 1

    assert len(child_a.inputs[0]) == 1
    assert len(child_a.inputs[1]) == 1
    assert len(child_b.inputs[0]) == 1
    assert len(child_b.inputs[1]) == 1

    assert child_a.inputs[0][0] == parent.outputs[0][0]
    assert child_a.inputs[1][0] == parent.outputs[1][0]
    assert child_b.inputs[0][0] == parent.outputs[0][1]
    assert child_b.inputs[1][0] == parent.outputs[1][1]


def test_fan_out_with_distributed():
    @task(output_types=(Int, Boolean))
    def parent():
        return 1, False
    
    @task(input_types=(Int,))
    def child_a(value):
        print(value)
    
    @task(input_types=(Boolean,))
    def child_b(value):
        print(value)

    manager = WorkflowManager()
    manager.add_workflow(parent |f| DistributedTasks(child_a, child_b))    

    assert len(parent.outputs[0]) == 1
    assert len(parent.outputs[1]) == 1

    assert parent.outputs[0][0].parent == parent
    assert parent.outputs[0][0].output_idx == 0
    assert parent.outputs[0][0].child == child_a
    assert parent.outputs[0][0].input_idx == 0

    assert parent.outputs[1][0].parent == parent
    assert parent.outputs[1][0].output_idx == 1
    assert parent.outputs[1][0].child == child_b
    assert parent.outputs[1][0].input_idx == 0

    assert len(child_a.inputs[0]) == 1
    assert len(child_b.inputs[0]) == 1

    assert child_a.inputs[0][0] == parent.outputs[0][0]
    assert child_b.inputs[0][0] == parent.outputs[1][0]

def test_branch_pipeline():
    @task(
        output_types=(Int, Condition, Boolean),
    )
    def branch_task():
        return 1, "child_a", True

    @task(input_types=(Int, Boolean))
    def child_a(a, b):
        print(a, b)
    
    @task(input_types=(Int, Boolean))
    def child_b(a, b):
        print(a, b)

    manager = WorkflowManager()
    manager.add_workflow(branch_task |b| Tasks(child_a, child_b))
  
    assert branch_task.branching
    assert len(branch_task.outputs[0]) == 2
    assert len(branch_task.outputs[2]) == 2

    assert branch_task.outputs[0][0].parent == branch_task
    assert branch_task.outputs[0][0].output_idx == 0
    assert branch_task.outputs[0][0].child == child_a
    assert branch_task.outputs[0][0].input_idx == 0

    assert branch_task.outputs[0][1].parent == branch_task
    assert branch_task.outputs[0][1].output_idx == 0
    assert branch_task.outputs[0][1].child == child_b
    assert branch_task.outputs[0][1].input_idx == 0

    assert branch_task.outputs[2][0].parent == branch_task
    assert branch_task.outputs[2][0].output_idx == 2
    assert branch_task.outputs[2][0].child == child_a
    assert branch_task.outputs[2][0].input_idx == 1

    assert branch_task.outputs[2][1].parent == branch_task
    assert branch_task.outputs[2][1].output_idx == 2
    assert branch_task.outputs[2][1].child == child_b
    assert branch_task.outputs[2][1].input_idx == 1

    assert len(child_a.inputs[0]) == 1
    assert len(child_a.inputs[1]) == 1
    assert child_a.inputs[0][0] == branch_task.outputs[0][0]
    assert child_a.inputs[1][0] == branch_task.outputs[2][0]

    assert len(child_b.inputs[0]) == 1
    assert len(child_b.inputs[1]) == 1
    assert child_b.inputs[0][0] == branch_task.outputs[0][1]
    assert child_b.inputs[1][0] == branch_task.outputs[2][1]


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
        start_task |s| scattered_task_a |f| scattered_task_b |g| gathered_task
    )

    assert not start_task.is_scattered()
    assert scattered_task_a.is_scattered()
    assert scattered_task_b.is_scattered()
    assert not gathered_task.is_scattered()

    assert len(start_task.outputs[0]) == 1
    assert start_task.outputs[0][0].is_scattered()
    assert start_task.outputs[0][0].parent == start_task
    assert start_task.outputs[0][0].output_idx == 0
    assert start_task.outputs[0][0].child == scattered_task_a
    assert start_task.outputs[0][0].input_idx == 0
    assert scattered_task_a.inputs[0][0] == start_task.outputs[0][0]
    
    assert len(scattered_task_a.outputs[0]) == 1
    assert not scattered_task_a.outputs[0][0].is_scattered()
    assert scattered_task_a.outputs[0][0].parent == scattered_task_a
    assert scattered_task_a.outputs[0][0].output_idx == 0
    assert scattered_task_a.outputs[0][0].child == scattered_task_b
    assert scattered_task_a.outputs[0][0].input_idx == 0
    assert scattered_task_b.inputs[0][0] == scattered_task_a.outputs[0][0]

    assert len(scattered_task_b.outputs[0]) == 1
    assert scattered_task_b.outputs[0][0].is_wrapped()
    assert scattered_task_b.outputs[0][0].parent == scattered_task_b
    assert scattered_task_b.outputs[0][0].output_idx == 0
    assert scattered_task_b.outputs[0][0].child == gathered_task
    assert scattered_task_b.outputs[0][0].input_idx == 0
    assert gathered_task.inputs[0][0] == scattered_task_b.outputs[0][0]


def test_task_to_runnable_script(): 
    @task(input_types=(Int, Boolean))
    def my_task(a, b):
        print(a, b)
    
    manager = WorkflowManager()
    manager.add_workflow(Values(Int(5), Boolean(True)) |f| my_task)
    manager.translate()

    with open("my_task.py", "r") as file:
        created = file.read()
    with open("tests/test_task_to_runnable_script.py", "r") as file:
        desired = file.read()
    
    assert created == desired
    os.remove("my_task.py")
    os.remove("wdl_script.wdl")


def test_temp2():
    @task(
        input_types=(Int, Boolean),
        output_types=(Int, Condition, Boolean),
    )
    def branch_task():
        return 1, "child_a", True

    @task(input_types=(Int, Boolean), output_types=(Int,))
    def child_a(a, b):
        return a
    
    @task(input_types=(Int, Boolean), output_types=(Int,))
    def child_b(a, b):
        return a
    
    @task(input_types=(Int,))
    def joined_task(a):
        print(a)

    manager = WorkflowManager()
    manager.add_workflow(Values(Int(1), Boolean(True)) |f| branch_task |b| Tasks(child_a, child_b) |j| joined_task)
    manager.translate()

    with open("wdl_script.wdl", "r") as file:
        created = file.read()
    with open("tests/test_workflow_to_wdl_script.wdl", "r") as file:
        desired = file.read()
    
    assert created == desired
    for t in manager.iterate_over_task():
        os.remove(f"{t.name}.py")
    os.remove("wdl_script.wdl")
