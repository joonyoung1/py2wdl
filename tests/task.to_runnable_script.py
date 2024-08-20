import sys


def my_task(a, b):
    print(a, b)


if __name__ == "__main__":
    sys.args[1] = int(sys.args[1])
    sys.args[2] = True if sys.args[2] == "true" else False
    outputs = my_task(*sys.args[1:])

    for i, output in enumerate(outputs):
        with open(f"my_task_output_{i}.txt", "w") as file:
            if isinstance(output, list):
                file.write("\n".join(map(str, output)))
            else:
                file.write(str(output))
