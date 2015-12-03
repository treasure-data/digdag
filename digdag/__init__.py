import sys
import collections

class Task(object):
    def __init__(self, config, subtask_config, state_params, export_params, carry_params):
        self.config = config
        self.subtask_config = subtask_config
        self.state_params = state_params
        self.export_params = export_params
        self.carry_params = carry_params
        self.subtask_index = 0

    def set_state(self, key, value):
        self.state_params[key] = value

    def export_param(self, key, value):
        self.export_params[key] = value

    def carry_param(self, key, value):
        self.carry_params[key] = value

    def add_subtask(self, function, **params):
        if hasattr(function, "im_class"):
            command = ".".join([function.im_class.__module__, function.im_class.__name__, function.__name__])
        else:
            command = ".".join([function.__module__, function.__name__])
        params["py>"] = command
        self.subtask_config["+subtask" + str(self.subtask_index)] = params
        self.subtask_index += 1

if 'digdag_env' in sys.modules:
    # executed by digdag-core/src/main/resources/digdag/standards/py/runner.py
    import digdag_env
    task = Task(digdag_env.config, digdag_env.subtask_config, digdag_env.state_params, digdag_env.export_params, digdag_env.carry_params)

else:
    task = Task({}, collections.OrderedDict, {}, {}, {})

class BaseTask(object):
    def __init__(self):
        self.subtask_config = task.subtask_config
        self.config = task.config
        self.state_params = task.state_params
        self.export_params = task.export_params
        self.carry_params = task.carry_params

    def set_state(self, key, value):
        task.set_state(key, value)

    def export_param(self, key, value):
        task.export_param(key, value)

    def carry_param(self, key, value):
        task.carry_param(key, value)

    def add_subtask(self, function, **params):
        task.add_subtask(function, **params)

