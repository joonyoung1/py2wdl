import sys


def my_task(a, b):
    print(a, b)


if __name__ == "__main__":
    sys.args[1] = int(sys.args[1])
    sys.args[2] = True if sys.args[2] == "true" else False
    my_task(*sys.args[1:])