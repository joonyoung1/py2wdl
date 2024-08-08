import pytest
from py2wdl.task import *


def test_basic_pipeline():
    @task(output_types=(Int, String))
    def first():
        return 1, "test"

    @task(input_types=(Int, String))
    def second(a, b):
        print(a, b)
    
    first | second

    a, b = first.get_outputs()
    assert a.parent_task == first
    assert a.output_idx == 0
    assert b.parent_task == first
    assert b.output_idx == 1

    assert len(a.children) == 1
    a_child = a.children[0]
    assert a_child[0] == second
    assert a_child[1] == 0

    assert len(b.children) == 1
    b_child = b.children[0]
    assert b_child[0] == second
    assert b_child[1] == 1


def test_value_input():
    @task(input_types=(Int, Int))
    def print_task(a, b):
        print(a + b)

    a, b = Int(1), Int(2)
    
    [a, b] | print_task

    assert len(a.children) == 1
    assert a.children[0][0] == print_task
    assert a.children[0][1] == 0

    assert len(b.children) == 1
    assert b.children[0][0] == print_task
    assert b.children[0][1] == 1


def test_fan_out_with_list():
    @task(output_types=(Boolean, File))
    def parent_task():
        return True, "test.txt"

    @task(input_types=(Boolean, File))
    def child_task_a(a, b):
        print(a, b)
    
    @task(input_types=(Boolean, File))
    def child_task_b(a, b):
        print(a, b)
    
    parent_task | [child_task_a, child_task_b]

    a, b = parent_task.get_outputs()
    assert a.parent_task == parent_task
    assert a.output_idx == 0
    assert b.parent_task == parent_task
    assert b.output_idx == 1

    assert len(a.children) == 2
    assert a.children[0][0] == child_task_a
    assert a.children[0][1] == 0
    assert a.children[1][0] == child_task_b
    assert a.children[1][1] == 0

    assert len(b.children) == 2
    assert b.children[0][0] == child_task_a
    assert b.children[0][1] == 1
    assert b.children[1][0] == child_task_b
    assert b.children[1][1] == 1


def test_fan_out_with_tuple():
    @task(output_types=(Int, Boolean))
    def parent_task():
        return 1, False
    
    @task(input_types=(Int,))
    def child_task_a(value):
        print(value)
    
    @task(input_types=(Boolean,))
    def child_task_b(value):
        print(value)
    
    parent_task | (child_task_a, child_task_b)

    a, b = parent_task.get_outputs()
    assert a.parent_task == parent_task
    assert a.output_idx == 0
    assert b.parent_task == parent_task
    assert b.output_idx == 1

    assert len(a.children) == 1
    assert a.children[0][0] == child_task_a
    assert a.children[0][1] == 0
    
    assert len(b.children) == 1
    assert b.children[0][0] == child_task_b
    assert b.children[0][1] == 0


def test_branch_pipeline():
    @task(
        output_types=(Int, Condition, Boolean),
        branch=True
    )
    def branch_task():
        return 1, "child_task_a", True

    @task(input_types=(Int, Boolean))
    def child_task_a(a, b):
        print(a, b)
    
    @task(input_types=(Int, Boolean))
    def child_task_b(a, b):
        print(a, b)
    
    branch_task > [child_task_a, child_task_b]

    condition = branch_task.condition
    assert condition.parent_task == branch_task
    assert condition.output_idx == 1

    a, b = branch_task.get_outputs()
    assert a.parent_task == branch_task
    assert a.output_idx == 0
    assert b.parent_task == branch_task
    assert b.output_idx == 2

    assert len(a.children) == 2
    assert a.children[0][0] == child_task_a
    assert a.children[0][1] == 0
    assert a.children[1][0] == child_task_b
    assert a.children[1][1] == 0

    assert len(b.children) == 2
    assert b.children[0][0] == child_task_a
    assert b.children[0][1] == 1
    assert b.children[1][0] == child_task_b
    assert b.children[1][1] == 1


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
    
    ((start_task << scattered_task_a) | scattered_task_b) >> gathered_task

    assert not start_task.is_scattered()
    assert len(start_task.outputs) == 1
    array = start_task.outputs[0]
    assert array.parent_task == start_task
    assert array.output_idx == 0
    
    assert len(array.children) == 0
    assert len(array.element.children) == 1
    assert array.element.children[0][0] == scattered_task_a
    assert array.element.children[0][1] == 0

    assert scattered_task_a.is_scattered()
    assert len(scattered_task_a.outputs) == 1
    element = scattered_task_a.outputs[0]
    assert element.parent_task == scattered_task_a
    assert element.output_idx == 0

    assert not element.is_wrapped()
    assert len(element.children) == 1
    assert element.array is None
    assert element.children[0][0] == scattered_task_b
    assert element.children[0][1] == 0

    assert scattered_task_b.is_scattered()
    assert len(scattered_task_b.outputs) == 1
    element = scattered_task_b.outputs[0]
    assert element.parent_task == scattered_task_b
    assert element.output_idx == 0

    assert element.is_wrapped()
    assert len(element.array.children) == 1
    assert element.array.children[0][0] == gathered_task
    assert element.array.children[0][1] == 0

    assert not gathered_task.is_scattered()