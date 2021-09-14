import sys
import os
import json
import types
import inspect
import collections
import traceback

command = sys.argv[1]
in_file = sys.argv[2]
out_file = sys.argv[3]

with open(in_file) as f:
    in_data = json.load(f)
    params = in_data['params']

# fake digdag_env module already imported
digdag_env_mod = sys.modules['digdag_env'] = types.ModuleType('digdag_env')
digdag_env_mod.params = params
digdag_env_mod.subtask_config = collections.OrderedDict()
digdag_env_mod.export_params = {}
digdag_env_mod.store_params = {}
digdag_env_mod.state_params = {}
import digdag_env

# fake digdag module already imported
digdag_mod = sys.modules['digdag'] = types.ModuleType('digdag')

class Env(object):
    def __init__(self, digdag_env_mod):
        self.params = digdag_env_mod.params
        self.subtask_config = digdag_env_mod.subtask_config
        self.export_params = digdag_env_mod.export_params
        self.store_params = digdag_env_mod.store_params
        self.state_params = digdag_env_mod.state_params
        self.subtask_index = 0

    def set_state(self, params={}, **kwds):
        self.state_params.update(params)
        self.state_params.update(kwds)

    def export(self, params={}, **kwds):
        self.export_params.update(params)
        self.export_params.update(kwds)

    def store(self, params={}, **kwds):
        self.store_params.update(params)
        self.store_params.update(kwds)

    def add_subtask(self, function=None, **params):
        if function is not None and not isinstance(function, dict):
            if hasattr(function, "im_class"):
                # Python 2
                command = ".".join([function.im_class.__module__, function.im_class.__name__, function.__name__])
            else:
                # Python 3
                command = ".".join([function.__module__, function.__qualname__])
            config = params
            config["py>"] = command
        else:
            if isinstance(function, dict):
                config = function.copy()
                config.update(params)
            else:
                config = params
        try:
            json.dumps(config)
        except Exception as error:
            raise TypeError("Parameters must be serializable using JSON: %s" % str(error))
        self.subtask_config["+subtask" + str(self.subtask_index)] = config
        self.subtask_index += 1

digdag_mod.env = Env(digdag_env_mod)
import digdag

# add the archive path to import path
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
            raise AttributeError("Module '%s' has no attribute '%s'" % (".".join(fragments), method_name))
    except ImportError as error:
        class_name = fragments.pop()
        mod = __import__(".".join(fragments), fromlist=[class_name])
        try:
            class_type = getattr(mod, class_name)
        except AttributeError as error:
            raise AttributeError("Module '%s' has no attribute '%s'" % (".".join(fragments), method_name))

    if type(callable_type) == type:
        class_type = callable_type
        method_name = "run"

    if class_type is not None:
        return (class_type, method_name)
    else:
        return (callable_type, None)

def digdag_inspect_arguments(callable_type, exclude_self, params):
    if callable_type == object.__init__:
        # object.__init__ accepts *varargs and **keywords but it throws exception
        return {}
    if hasattr(inspect, 'getfullargspec'): # Python3
        spec = inspect.getfullargspec(callable_type)
        keywords_ = spec.varkw
    else: # Python 2
        spec = inspect.getargspec(callable_type)
        keywords_ = spec.keywords

    args = {}
    for idx, key in enumerate(spec.args):
        if exclude_self and idx == 0:
            continue
        if key in params:
            args[key] = params[key]
        else:
            if spec.defaults is None or idx < len(spec.args) - len(spec.defaults):
                # this keyword is required but not in params. raising an error.
                if hasattr(callable_type, '__qualname__'):
                    # Python 3
                    name = callable_type.__qualname__
                elif hasattr(callable_type, 'im_class'):
                    # Python 2
                    name = "%s.%s" % (callable_type.im_class.__name__, callable_type.__name__)
                else:
                    name = callable_type.__name__
                raise TypeError("Method '%s' requires parameter '%s' but not set" % (name, key))
    if keywords_:
        # above code was only for validation
        return params
    else:
        return args

error = None
error_message = None
error_value = None
error_traceback = None
callable_type = None
method_name = None

try:
    callable_type, method_name = digdag_inspect_command(command)
    if method_name:
        init_args = digdag_inspect_arguments(callable_type.__init__, True, params)
        instance = callable_type(**init_args)

        method = getattr(instance, method_name)
        method_args = digdag_inspect_arguments(method, True, params)
        result = method(**method_args)
    else:
        args = digdag_inspect_arguments(callable_type, False, params)
        result = callable_type(**args)
except SystemExit as e:
    # SystemExit only shows an exit code and it is not kind to users. So this block creates a specific error message.
    # This error will happen if called python module name and method name are equal to those of the standard library module. (e.g. tokenize.main)
    error = Exception("Python command %s terminated with exit code %d" % (command, e.code), "Possible cause: program intentionally/accidentally finished, or module name conflicted with the standard library")
    error_type, error_value, _tb = sys.exc_info()
    error_message = "%s %s" % (error.args[0], error.args[1])
    error_traceback = traceback.format_exception(error_type, error_value, _tb)
except Exception as e:
    error = e
    error_type, error_value, _tb = sys.exc_info()
    error_message = str(error_value)
    error_traceback = traceback.format_exception(error_type, error_value, _tb)

out = {
    'subtask_config': digdag_env.subtask_config,
    'export_params': digdag_env.export_params,
    'store_params': digdag_env.store_params,
    #'state_params': digdag_env.state_params,  # only for retrying
}

if error:
    out['error'] = {
        'class': error_value.__class__.__name__,
        'message': error_message,
        'backtrace': error_traceback
    }

with open(out_file, 'w') as f:
    json.dump(out, f)

if error:
    raise error
