import pytest
from py2wdl.task import *


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


def test_connection_using_simple_variable():
    a = Int(1)
    b = Int(2)
    result = task_add(a, b)
    task_print(result)

    assert result.parent_task == task_add
    assert result.output_idx == 0
    
    child = result.child[0]
    assert child[0] == task_print
    assert child[1] == 0


def test_connection_using_array_variable():
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
        assert len(num.child) == 2
        
        child_1, child_2 = num.child
        assert child_1[0] == task_add
        assert child_1[1] == 0
        assert child_2[0] == task_add
        assert child_2[1] == 1