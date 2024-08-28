class Infix:
    def __init__(self, function):
        self.function = function
    
    def __ror__(self, other):
        return Infix(lambda x: self.function(other, x))
    
    def __or__(self, other):
        return self.function(other)


f = Infix(lambda x,y: x.forward(y))
b = Infix(lambda x,y: x.branch(y))
j = Infix(lambda x,y: x.join(y))
s = Infix(lambda x,y: x.scatter(y))
g = Infix(lambda x,y: x.gather(y))

forward = f
branch = b
join = j
scatter = s
gather = g