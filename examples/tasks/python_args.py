import digdag

def required_arguments(required1, required2):
    print("required1 = " + str(required1))
    print("required2 = " + str(required2))

def optional_arguments(optional1=None, optional2="default"):
    print("optional1 = " + str(optional1))
    print("optional2 = " + str(optional2))

def mixed_arguments(arg1, arg2=None, arg3=None):
    print("arg1 = " + str(arg1))
    print("arg2 = " + str(arg2))
    print("arg3 = " + str(arg3))

def keyword_arguments(arg1, **kw):
    print("arg1 = " + str(arg1))
    print("keywords = " + str(kw))

