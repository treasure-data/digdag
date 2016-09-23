from dateutil import parser
from random import randint

class MyClass(object):
    def __init__(self):
        pass

    def print_time(self, label, time):
        print([label, parser.parse(time)])

    def say_something(self, order, animal):
        print("""%s %s said "My favorite number is %d" """ % (order, animal, randint(0, 9)))

