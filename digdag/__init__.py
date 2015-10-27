import inspect

class BaseTask(object):
    def __init__(self, config, state, params):
        self.config = config
        self.state = state
        self.params = params
        self.carry_params = {}
        self.sub = {}
        self.subtask_index = 0
        self.inputs = []
        self.outputs = []

        spec = inspect.getargspec(self.init)

        if spec.keywords:
            args = config
        else:
            keys = spec.args[1:]
            args = dict()
            for key in keys:
                if key in config:
                    args[key] = config[key]
        self.init(**args)

    def init(self):
        pass

    def carry_param(self, key, value):
        self.carry_params[key] = value

    def add_subtask(self, pyclass, **config):
        config["py>"] = pyclass.__module__ + "." + pyclass.__name__
        self.sub["+subtask" + str(self.subtask_index)] = config
        self.subtask_index += 1

