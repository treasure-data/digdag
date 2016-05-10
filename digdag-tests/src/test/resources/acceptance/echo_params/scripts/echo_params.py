from __future__ import print_function
import digdag

def echo_params():
    print('digdag params')
    for k, v in digdag.env.params.items():
        print(k, v)

