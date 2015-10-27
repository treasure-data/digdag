import json
import re
import os.path
import errno
import digdag

class SplitFiles(digdag.BaseTask):
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

class CheckFiles(digdag.BaseTask):
    def run(self):
        for path in self.params["paths"]:
            if not os.path.isfile(path):
                raise Exception("File not built: " + path)

class PrintFiles(digdag.BaseTask):
    def run(self):
        print "Creating subtasks for files: "+str(self.params['paths'])

        for path in self.params['paths']:
            try:
                os.makedirs(os.path.dirname(path))
            except OSError as exc:
                pass
            with open(path, "w") as f:
                json.dump({"data": path}, f)
            self.add_subtask(PrintFilesSub, path=path)
            self.inputs.append({"file": path})  # reports data lineage tracking
        self.sub["parallel"] = True

class PrintFilesSub(digdag.BaseTask):
    def init(self, path):
        self.path = path

    def run(self):
        #if self.path == "tmp/out.2":
        #    raise Exception("error at PrintFilesSub")

        with open(self.path) as f:
            data = json.load(f)
        print "Printing file " + self.path + "...: " + str(data)

