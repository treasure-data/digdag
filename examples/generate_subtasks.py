import digdag

class Split(digdag.BaseTask):
    def run(self):
        self.carry_params["task_count"] = 3

class ParallelProcess(digdag.BaseTask):
    def run(self):
        for i in range(self.params["task_count"]):
            self.add_subtask(ProcessSub, index=i)
        self.sub["parallel"] = True

class ProcessSub(digdag.BaseTask):
    def init(self, index=None):
        self.index = index

    def run(self):
        print "Processing " + str(self.index)

