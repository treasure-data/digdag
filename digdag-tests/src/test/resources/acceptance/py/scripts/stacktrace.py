class MyError(Exception):
    pass

def run():
    raise MyError('my error message')
