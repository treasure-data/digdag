import sys
import os
import json
import imp
import inspect
import collections

command = sys.argv[1]
in_file = sys.argv[2]
out_file = sys.argv[3]

with open(in_file) as f:
    in_data = json.load(f)
    config = in_data['config']

mod = sys.modules['digdag_env'] = imp.new_module('digdag_env')
mod.config = config
mod.subtask_config = collections.OrderedDict()
mod.state_params = {}
mod.export_params = {}
mod.carry_params = {}
import digdag_env

sys.path.append(os.path.abspath(os.getcwd()))

def digdag_inspect_command(command):
    # package.name.Class.method
    fragments = command.split(".")
    method_name = fragments.pop()
    class_type = None
    callable_type = None
    try:
        mod = __import__(".".join(fragments), fromlist=[method_name])
        try:
            callable_type = getattr(mod, method_name)
        except AttributeError as error:
            raise AttributeError("Module '"+".".join(fragments)+"' has no attribute '"+method_name+"'")
    except ImportError as error:
        class_name = fragments.pop()
        mod = __import__(".".join(fragments), fromlist=[class_name])
        try:
            class_type = getattr(mod, class_name)
        except AttributeError as error:
            raise AttributeError("Module '"+".".join(fragments)+"' has no attribute '"+class_name+"'")

    if type(callable_type) == type:
        class_type = callable_type
        method_name = "run"

    if class_type is not None:
        return (class_type, method_name)
    else:
        return (callable_type, None)

def digdag_inspect_arguments(callable_type, exclude_self, config):
    spec = inspect.getargspec(callable_type)
    if spec.keywords:
        return config
    else:
        if exclude_self:
            keys = spec.args[1:]
        else:
            keys = spec.args
        args = {}
        for key in keys:
            if key in config:
                args[key] = config[key]
            else:
                args[key] = None
        return args

callable_type, method_name = digdag_inspect_command(command)

if method_name:
    init_args = digdag_inspect_arguments(callable_type.__init__, True, config)
    instance = callable_type(**init_args)

    method = getattr(instance, method_name)
    method_args = digdag_inspect_arguments(method, True, config)
    result = method(**method_args)

else:
    args = digdag_inspect_arguments(callable_type, False, config)
    result = callable_type(**args)

out = {
    'subtask_config': digdag_env.subtask_config,
    'export_params': digdag_env.export_params,
    'state_params': digdag_env.state_params,
    'carry_params': digdag_env.carry_params,
}

with open(out_file, 'w') as f:
    json.dump(out, f)

