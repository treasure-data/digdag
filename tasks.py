import inspect
import json
import re

# this should be in a library code
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




import os.path

class SplitFiles(BaseTask):
    def init(self, count=None):
        self.count = count

    def run(self):
        print "Splitting files...: count="  + str(self.count)

        paths = []
        for i in range(self.count):
            path = "tmp/out." + str(i)
            paths.append(path)
            self.outputs.append({"file": path})  # reports data lineage tracking

        self.carry_params["paths"] = paths

class CheckFiles(BaseTask):
    def run(self):
        for path in self.params["paths"]:
            if not os.path.isfile(path):
                raise Exception("File not built: " + path)

class PrintFiles(BaseTask):
    def run(self):
        print "Creating subtasks for files: "+str(self.params['paths'])

        for path in self.params['paths']:
            with open(path, "w") as f:
                json.dump({"data": path}, f)
            self.add_subtask(PrintFilesSub, path=path)
            self.inputs.append({"file": path})  # reports data lineage tracking
        self.sub["parallel"] = True

class PrintFilesSub(BaseTask):
    def init(self, path):
        self.path = path

    def run(self):
        #if self.path == "tmp/out.2":
        #    raise Exception("error at PrintFilesSub")

        with open(self.path) as f:
            data = json.load(f)
        print "Printing file " + self.path + "...: " + str(data)

