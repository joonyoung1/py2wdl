class WDLType:
    def __init__(self) -> None:
        self.name: str = str(id(self))

class Boolean(WDLType):
    def __init__(self) -> None:
        super().__init__()

class Int(WDLType):
    def __init__(self) -> None:
        super().__init__()

class String(WDLType):
    def __init__(self) -> None:
        super().__init__()

class File(WDLType):
    def __init__(self) -> None:
        super().__init__()

class Condition(WDLType):
    def __init__(self) -> None:
        super().__init__()