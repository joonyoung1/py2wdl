import pytest
from py2wdl.task import *


def create_task():
    @task(
        input_types=(Int, Int),
        output_types=(Int,),
    )
    def task_add(a, b):
        return a + b

    @task(input_types=(Int,))
    def task_print(num):
        print(num)

    @task(output_types=(Array[Int],))
    def task_generate_array():
        return [i for i in range(10)]

    return task_add, task_print, task_generate_array


def test_connection_using_simple_variable():
    task_add, task_print, task_generate_array = create_task()

    a = Int(1)
    b = Int(2)
    result = task_add(a, b)
    task_print(result)

    assert result.parent_task == task_add
    assert result.output_idx == 0

    child = result.children[0]
    assert child[0] == task_print
    assert child[1] == 0


def test_connection_using_array_variable():
    task_add, task_print, task_generate_array = create_task()

    array = task_generate_array()
    assert isinstance(array, Array)
    assert array.parent_task == task_generate_array
    assert array.output_idx == 0
    assert array.element_type == Int

    for num in array:
        result = task_add(num, num)
        assert isinstance(result, Int)
        assert result.parent_task == task_add
        assert result.output_idx == 0
        assert len(num.children) == 2

        child_1, child_2 = num.children
        assert child_1[0] == task_add
        assert child_1[1] == 0
        assert child_2[0] == task_add
        assert child_2[1] == 1


def test_pipeline_operation():
    task_add, task_print, task_generate_array = create_task()
    
    a = Int(1)
    b = Int(2)

    [a, b] | task_add | task_print

    a_child = a.children[0]
    assert len(a.children) == 1
    assert a_child[0] == task_add
    assert a_child[1] == 0

    b_child = b.children[0]
    assert len(b.children) == 1
    assert b_child[0] == task_add
    assert b_child[1] == 1

    output = task_add.outputs[0]
    assert len(task_add.outputs) == 1
    assert output.parent_task == task_add
    assert output.output_idx == 0

    child = output.children[0]
    assert len(output.children) == 1
    assert child[0] == task_print
    assert child[1] == 0