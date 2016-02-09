from __future__ import print_function
import digdag

def set_my_param():
    digdag.env.export_params["my_param"] = {"key": "value"}

def show_my_param(my_param):
    print("my_param = " + str(my_param))

