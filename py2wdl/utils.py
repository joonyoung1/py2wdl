def is_iterable(var):
    try:
        iter(var)
        return True
    except TypeError:
        return False
