import digdag

def fails():
    raise Exception("This task fails")

def show_error(error=None):
    print("OK, task failed expectedly:")
    print("  * error: " + str(error))

