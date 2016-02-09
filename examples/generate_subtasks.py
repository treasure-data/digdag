from __future__ import print_function
import digdag

class ParallelProcess(digdag.BaseTask):
    def split(self):
        self.export_params["task_count"] = 3

    def run(self, task_count):
        for i in range(task_count):
            self.add_subtask(ParallelProcess.subtask, index=i)
        self.subtask_config["parallel"] = True

    def subtask(self, index):
        print("Processing " + str(index))

