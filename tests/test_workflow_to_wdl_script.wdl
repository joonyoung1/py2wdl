task branch_task {
    input {
        Int input_0
        Boolean input_1
    }
    command {
        python branch_task.py ${input_0} ${input_1}
    }
    output {
        Int branch_task_output_0 = read_int(branch_task_output_0.txt)
        String branch_task_output_1 = read_string(branch_task_output_1.txt)
        Boolean branch_task_output_2 = read_boolean(branch_task_output_2.txt)
    }
}
task child_a {
    input {
        Int input_0
        Boolean input_1
    }
    command {
        python child_a.py ${input_0} ${input_1}
    }
    output {
        Int child_a_output_0 = read_int(child_a_output_0.txt)
    }
}
task child_b {
    input {
        Int input_0
        Boolean input_1
    }
    command {
        python child_b.py ${input_0} ${input_1}
    }
    output {
        Int child_b_output_0 = read_int(child_b_output_0.txt)
    }
}
task joined_task {
    input {
        Int input_0
    }
    command {
        python joined_task.py ${input_0}
    }
}
workflow my_workflow {
    input {
        Int Values2_output_0 = 1
        Boolean Values2_output_1 = true
    }
    call branch_task {
        input:
            input_0 = Values2_output_0,
            input_1 = Values2_output_1,
    }
    if branch_task.output_1 == "child_b" {
        call child_b {
            input:
                input_0 = branch_task.output_0,
                input_1 = branch_task.output_2,
        }
        joined_task_input_0 = child_b.output_0
    }
    else if branch_task.output_1 == "child_a" {
        call child_a {
            input:
                input_0 = branch_task.output_0,
                input_1 = branch_task.output_2,
        }
        joined_task_input_0 = child_a.output_0
    }
    call joined_task {
        input:
            input_0 = joined_task_input_0,
    }
}
