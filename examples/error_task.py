from __future__ import print_function
import digdag

def fails():
    raise Exception("This task fails")

def show_error(error_message=None, error_task_name=None):
    print("Task failed:")
    print("  * error_message: " + str(error_message))
    print("  * error_task_name: " + str(error_task_name))

